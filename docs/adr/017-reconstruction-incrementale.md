# ADR 017 : reconstruction incrémentale de silver et gold

- **Statut** : accepté
- **Date** : novembre 2026

## Contexte

Le DAG `dbt_transformations` reconstruit silver et gold en entier, toutes les heures (`CREATE OR REPLACE TABLE`). Le calcul est rapide : 3,5 s pour silver (522 231 lignes), 0,5 s pour gold. Le stockage, lui, ne l'est pas. Mesuré dans SeaweedFS le 23 novembre 2026 :

| Namespace | Table vivante | Sur disque |
| --- | --- | --- |
| `silver` | 14,1 Mo | 1 476 Mo |
| `gold` | 0,39 Mo | 161 Mo |
| `bronze` | 14,5 Mo | 102 Mo |

Chaque run écrit une copie complète de silver, soit 14 Mo de fichiers par heure. Nessie GC garde tout ce que référence un commit des 7 derniers jours (ADR 013) : environ 105 copies de silver. **1,6 Go des 1,74 Go du lac sont d'anciennes copies de silver et gold.** Entre deux runs, pourtant, seules quelques lignes changent : un rafraîchissement du temps réel en apporte 14 en médiane (ADR 010), un mois d'historique rechargé environ 1 500.

## Critères

1. **Résultat identique** à une reconstruction complète, ligne pour ligne.
2. **Rien de manqué**, même quand deux chargements se croisent (DAG mensuel et rechargement du dimanche, ADR 015) ou qu'un commit arrive tard.
3. **Write-Audit-Publish inchangé** (ADR 008) : le modèle écrit sur la branche `audit`, les tests décident de la publication.
4. **Simple à expliquer et à tester.**

## Options étudiées

### Réduire le volume

| Option | Avantages | Limites |
| --- | --- | --- |
| Garder la reconstruction complète, raccourcir la rétention de Nessie GC | Aucun changement de modèle | La rétention est globale : bronze perd aussi son historique, et le voyage dans le temps se réduit d'autant |
| Reconstruire moins souvent | Simple | Silver et gold moins frais, alors que le temps réel arrive tous les quarts d'heure |
| **Silver incrémental (`merge`)** | Quelques Ko par run au lieu de 14 Mo | Un modèle plus complexe, des fichiers de suppression à compacter |

### Repérer les lignes à recalculer

| Option | Avantages | Limites |
| --- | --- | --- |
| Filigrane : lignes bronze ingérées après la plus récente de silver | Une comparaison simple | Un chargement commité après des lignes plus récentes est manqué. Avec une marge (3 h), tout le dernier chargement d'historique est recalculé à chaque run : l'historique entier, chargé le 9 novembre en 37 minutes, revenait dans chaque lot (514 241 lignes, mesuré) |
| **Anti-jointure : lignes bronze sans ligne silver de même quart d'heure ingérée au même moment ou plus tard** | Exact, ligne par ligne, sans marge ni course entre chargements | Deux jointures d'environ 500 000 lignes à chaque run (quelques secondes, sans écriture) |

## Décision

**`eco2mix_national` devient un modèle incrémental** : `materialized: incremental`, `incremental_strategy: merge`, `unique_key: timestamp_utc`, `on_schema_change: fail`.

**À chaque run, les quarts d'heure à recalculer sont ceux d'une ligne bronze que silver n'a pas encore vue** : aucune ligne silver pour le même quart d'heure n'a un `ingested_at` égal ou plus récent. Pour ces quarts d'heure seulement, le modèle relit **toutes** les lignes bronze des deux sources, et applique la logique complète : la dernière version temps réel, puis la priorité de l'historique. Le lot est fusionné dans silver sur `timestamp_utc`.

- **Une nouvelle version temps réel** (révision de RTE) est plus récente que la ligne de silver : son quart d'heure est recalculé, et la version la plus récente l'emporte.
- **Un mois d'historique rechargé** a un `ingested_at` plus récent que les lignes de silver : ses quarts d'heure sont recalculés, et la valeur consolidée remplace la valeur temps réel par le `merge`.
- **Une ligne déjà fusionnée** a le même `ingested_at` que la ligne de silver : elle n'est jamais recalculée.

**`dbt build --full-refresh` reconstruit la table en entier** (`CREATE OR REPLACE TABLE`, comme avant).

Vérifié sur les données du cluster, sans écriture, en exécutant le lot compilé dans Trino :

- le lot contient 14 lignes, exactement celles du dernier lot du writer, ingérées après le run précédent ;
- ces 14 lignes sont identiques à celles d'une reconstruction complète (`EXCEPT` vide) ;
- la première version, à filigrane et marge, recalculait 514 241 lignes : elle a été abandonnée pour l'anti-jointure.

Un test unitaire en mode incrémental (`is_incremental: true`, une table `this` simulée) couvre une version temps réel plus récente, un quart d'heure nouveau, un rechargement d'historique ingéré avant les dernières lignes temps réel, et deux lignes déjà vues. Comparer les quarts d'heure seuls, ou exiger une ligne silver strictement plus récente, le fait échouer.

## Conséquences

- **Un run n'écrit plus que les quarts d'heure modifiés**, quelques Ko au lieu de 14 Mo. Les anciennes copies de silver disparaîtront avec la rétention de 7 jours.
- **Le `merge` d'Iceberg laisse de petits fichiers et des fichiers de suppression** : silver doit être compacté chaque jour, comme bronze (ADR 013).
- **Un quart d'heure supprimé de bronze resterait dans silver** : l'anti-jointure ne voit que les lignes présentes. La source ne retire pas de quart d'heure aujourd'hui (un rechargement remplace les lignes d'un mois par d'autres, pour les mêmes quarts d'heure). Une reconstruction complète hebdomadaire sert de filet de sécurité.

## Mise à jour : gold quotidien

`energy_mix_daily` suit le même principe, par jour (`merge` sur `local_date`). `renewable_share_monthly` reste reconstruite en entier : 179 lignes, 13 Ko.

**Une colonne de plus, `last_ingested_at`** : la plus récente ingestion des lignes silver du jour. Une ligne silver modifiée est fusionnée avec l'heure d'ingestion de sa nouvelle ligne bronze, plus récente que toutes celles du jour : `last_ingested_at` avance. Un jour est donc recalculé quand silver a une ligne ingérée après son `last_ingested_at`, ou quand gold n'a pas encore ce jour.

**Les jours encore partiels sont recalculés aussi** : à minuit, la veille devient complète sans qu'aucune ligne n'arrive (ADR 012). Sans cela, `is_partial` resterait vrai pour toujours sur chaque jour passé.

**La transition** : la table actuelle n'a pas `last_ingested_at`. La macro `this_has_column` le détecte : le premier run recalcule alors tous les jours, et `on_schema_change: append_new_columns` ajoute la colonne. Un piège évité en chemin : un test unitaire qui simule la table (`this`) par des lignes YAML est vérifié contre les colonnes de la table réelle, et aurait échoué tant que la colonne n'existe pas. Or `dbt build` lance les tests unitaires avant le modèle : le premier run après le déploiement aurait échoué, la colonne ne serait jamais apparue, et la publication serait restée bloquée. La table simulée est donc écrite en SQL (`format: sql`), qui n'est pas vérifié ainsi.

Vérifié sur les données du cluster, sans écriture :

- le premier lot couvre les 5 441 jours, identiques au gold actuel sur ses 24 colonnes, avec `last_ingested_at` rempli ;
- en prenant ce lot comme gold, le run suivant ne recalculerait qu'un jour : le jour en cours, partiel.

Le test unitaire en mode incrémental couvre un jour révisé, un jour partiel devenu complet, un jour nouveau et un jour inchangé. Ne recalculer que les jours modifiés, ou que les jours absents, le fait échouer.

## Mise à jour : compactage et reconstruction hebdomadaire

**Silver et le gold quotidien sont compactés chaque jour, par le DAG `dbt_transformations` lui-même**, au run de 11:00 UTC, après la publication (macro `optimize_silver_gold_tables`, sur `main`). Pas par `iceberg_maintenance`, qui compacte bronze : son commit sur silver pourrait tomber entre la création de la branche `audit` et la fusion d'un run dbt, et Nessie refuserait la fusion, silver ayant changé des deux côtés (ADR 008). Ce cas arriverait presque chaque matin, quand les deux DAG rattrapent leurs runs au réveil du portable. Dans le même DAG, après `publish`, avec un seul run à la fois, aucune construction ne peut être en cours.

**Le run du dimanche à 11:00 UTC reconstruit silver et gold en entier** (`--full-refresh`), comme un run déclenché avec la configuration `{"full_refresh": true}`. C'est le filet de sécurité pour ce que l'anti-jointure ne voit pas, comme un quart d'heure supprimé de bronze.

Les deux règles sont des fonctions de l'heure logique du run, dans `wattlake_ingestion.transform_schedule`, testées sans Airflow. Avec `catchup=False`, un portable réveillé après midi saute le compactage du jour, ou la reconstruction de la semaine : le suivant les rattrape.

## Sources

- dbt, modèles incrémentaux et stratégie `merge`. https://docs.getdbt.com/docs/build/incremental-models
- dbt-trino 1.10, stratégies incrémentales sur Iceberg (`append`, `delete+insert`, `merge`). https://github.com/starburstdata/dbt-trino
- dbt, tests unitaires des modèles incrémentaux (`overrides.macros.is_incremental`, entrée `this`). https://docs.getdbt.com/docs/build/unit-tests
- Trino, `MERGE` sur le connecteur Iceberg. https://trino.io/docs/current/connector/iceberg.html
