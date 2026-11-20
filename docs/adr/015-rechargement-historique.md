# ADR 015 : rechargement de l'historique récent

- **Statut** : accepté
- **Date** : novembre 2026

## Contexte

L'historique éCO2mix (`eco2mix-national-cons-def`) publie chaque mois plusieurs fois, dans des états successifs (voir [docs/sources.md](../sources.md)) :

- les **données consolidées** arrivent avec deux à trois mois de retard. Le 20 novembre 2026, le dernier mois publié est août 2026 ;
- les **données définitives** remplacent les consolidées au second semestre de l'année suivante, soit jusqu'à environ 23 mois après le mois concerné.

Le DAG `eco2mix_history_to_bronze` charge chaque mois une seule fois, au début du mois suivant. À ce moment-là, la source ne contient pas encore le mois : la tâche se termine en `skipped`, et Airflow ne la relance jamais. Le 9 novembre 2026, le rattrapage a ainsi marqué septembre et octobre 2026 comme `skipped`. De même, un mois chargé en données consolidées n'est jamais rechargé en données définitives.

Silver garde la version temps réel d'un quart d'heure tant que l'historique ne le contient pas (ADR 012). Sans rechargement, le temps réel resterait donc pour toujours dans silver et gold à la place des données consolidées, puis définitives.

## Critères

1. **Chaque mois finit par contenir la dernière version publiée par RTE**, consolidée puis définitive, sans intervention.
2. **Pas d'écriture inutile** : un mois inchangé ne crée ni commit Nessie, ni exécution du DAG dbt.
3. **Le rattrapage initial depuis 2012 ne coûte pas plus cher.**
4. **Pas d'écriture concurrente non maîtrisée** sur la table bronze.
5. **Tout reste dans le dépôt** (GitOps) : aucune configuration faite à la main dans Airflow.

## Options étudiées

| Option | Avantages | Limites |
| --- | --- | --- |
| Un DAG dédié qui recharge chaque semaine les 24 derniers mois, en n'écrivant que les mois modifiés | Le DAG mensuel garde son rôle (premier chargement, rattrapage). Couvre les données consolidées et définitives | Un DAG de plus, et une comparaison à écrire et à tester |
| Le DAG mensuel recharge aussi les 23 mois précédents | Pas de nouveau DAG, et `max_active_runs=1` empêche toute écriture concurrente | Le rattrapage depuis 2012 retélécharge chaque mois 24 fois. Les données consolidées attendent jusqu'au 1er du mois suivant leur publication |
| Un DAG qui relance (`clear`) les exécutions `skipped` récentes du DAG mensuel | Très peu de code | Ne traite pas les données définitives : ces exécutions sont en `success`. Repose sur l'état d'Airflow, pas sur les données |

Pour éviter que les deux DAG écrivent en même temps dans la table, j'ai envisagé un pool Airflow d'un seul emplacement, partagé par leurs tâches. Le chart Helm d'Airflow ne déclare pas de pools : il faudrait le créer à la main ou par un job en plus, hors du dépôt (critère 5).

## Décision

**Un DAG `eco2mix_history_refresh`** ([ingestion/dags/eco2mix_history_refresh.py](../../ingestion/dags/eco2mix_history_refresh.py)) :

- **Le dimanche à 04:00, heure de Paris**, loin du chargement mensuel (le 1er à 00:00) et de `iceberg_maintenance` (11:30 UTC).
- **Les 24 mois complets avant le mois en cours**, une tâche par mois (*dynamic task mapping*), du plus ancien au plus récent, une à la fois (`max_active_tis_per_dagrun=1`).
- **Une écriture seulement si le contenu change.** Chaque tâche télécharge son mois, le relit dans la table et compare les colonnes de la source, sans les colonnes techniques et sans tenir compte de l'ordre (`skip_unchanged`, dans [bronze.py](../../ingestion/src/wattlake_ingestion/bronze.py)). Les lignes sont comparées comme un multiensemble : la source contient des `date_heure` en double au changement d'heure de printemps. Un mois relu fait environ 3 000 lignes : la comparaison coûte peu.
- **Un mois modifié est remplacé** par le même `overwrite` que le chargement mensuel, en un commit, et met à jour l'Asset de la table bronze, qui déclenche `dbt_transformations`. Un mois inchangé ou pas encore publié se termine en `skipped`, sans commit ni Asset.
- **Pas de pool** : les deux DAG peuvent encore écrire en même temps, quand le 1er du mois tombe un dimanche ou lors d'une exécution manuelle. Nessie refuse alors l'un des deux commits (concurrence optimiste), et sa tâche est relancée par Airflow (`retries=2`). C'est le même fonctionnement qu'entre le compactage et le writer (ADR 013).

## Conséquences

- **Les données consolidées arrivent dans bronze au plus une semaine après leur publication**, puis dans silver et gold à l'exécution dbt qui suit. Les données définitives remplacent les consolidées de la même façon.
- **Environ 24 exports par semaine** à l'API ODRE, de quelques centaines de Ko chacun.
- **Les mois manqués par le DAG mensuel restent `skipped` dans son historique** : c'est ce DAG qui les charge désormais. Le DAG mensuel ne sert plus qu'au premier chargement et au rattrapage.
- **Un nouveau DAG à activer** sur un cluster neuf, comme les autres (voir le [README](../../README.md)).
- **Une révision de plus de 24 mois n'est pas reprise.** RTE n'en publie pas en principe ; sinon, une exécution manuelle du DAG mensuel pour le mois concerné la charge.
- Je réexaminerai cette décision si RTE change son calendrier de publication, ou si la table est partitionnée par mois : le remplacement d'un mois deviendrait alors un remplacement de partition.
