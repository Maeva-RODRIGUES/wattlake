# ADR 012 : raccordement du temps réel aux couches silver et gold

- **Statut** : accepté
- **Date** : novembre 2026

## Contexte

Les mesures temps réel arrivent dans `bronze.eco2mix_national_realtime` (ADR 010), mais silver et gold ne lisent que l'historique, `bronze.eco2mix_national_history`. Les questions du projet, comme la part des renouvelables, s'arrêtent donc au 31 août 2026, dernier jour de l'historique consolidé.

Mesures faites sur les deux tables bronze, le 13 novembre 2026 :

- **Raccord** : l'historique s'arrête au 31 août 21:45 UTC, et le temps réel commence au 31 août 22:00 UTC (1ᵉʳ septembre 00:00, heure de Paris). Il n'y a ni trou ni chevauchement aujourd'hui. Quand RTE consolidera les mois suivants, l'historique couvrira des quarts d'heure déjà présents en temps réel.
- **Pas de temps** : dans l'historique, les valeurs réalisées sont publiées à la demi-heure (lignes :00 et :30, les lignes :15 et :45 sont vides). Dans le temps réel, elles le sont à chaque quart d'heure.
- **Versions** : le temps réel garde chaque version d'un quart d'heure (révisions de RTE, fenêtre de recouvrement du producteur), soit 7 489 lignes pour 6 982 quarts d'heure.
- **Changements d'heure** : le temps réel omet lui aussi l'une des deux heures répétées en octobre (96 quarts d'heure le 25 octobre 2026 au lieu de 100, particularité 8 de [docs/sources.md](../sources.md)).
- **Champs** : quatre champs n'existent que dans le temps réel (éolien terrestre et en mer, charge et décharge des batteries). Le run-of-river est publié en texte dans le temps réel, en entier dans l'historique.

## Critères

1. **Une seule version par quart d'heure** dans silver, la plus fiable disponible.
2. **Énergies exactes** quel que soit le pas de temps, sans jeter de mesures.
3. **L'historique ne change pas** : les jours jusqu'au 31 août gardent exactement leurs valeurs.
4. **Le jour en cours ne fait pas échouer les tests**, mais une donnée manquante un jour passé les fait toujours échouer.
5. **Peu de modèles** : silver reste la table de référence, lue par gold et par les utilisateurs.

## Options étudiées

### Où réunir les deux sources

| Option | Avantages | Limites |
| --- | --- | --- |
| Une seule table silver, qui réunit les deux sources | Une ligne par quart d'heure, quelle que soit la source. La priorité entre sources est réglée à un seul endroit. Gold et les utilisateurs ne voient qu'une table | Le modèle silver lit deux sources |
| Un modèle silver par source, réunis dans gold | Chaque modèle reste simple | Deux tables à garder cohérentes. La priorité de l'historique se règle dans gold, et chaque nouvel utilisateur doit la refaire |

### Calcul de l'énergie

| Option | Avantages | Limites |
| --- | --- | --- |
| Une colonne `step_hours` par ligne (0,5 h pour l'historique, 0,25 h pour le temps réel), énergie = Σ puissance × `step_hours` | Exact pour les deux sources, aucune mesure perdue. La complétude se compte en heures couvertes | `energy_mix_daily`, le modèle mensuel et deux tests sont réécrits |
| Ne garder du temps réel que les lignes :00 et :30, multipliées par 0,5 h | Gold ne change presque pas | La moitié des mesures temps réel est ignorée, et l'énergie devient une approximation |

### Le jour en cours

| Option | Avantages | Limites |
| --- | --- | --- |
| Colonne `is_partial` pour le jour en cours (et le mois en cours), ignorée par le test des jours incomplets | Le jour en cours est visible dans gold, ses totaux grandissent à chaque exécution | Une table gold dont une ligne change à chaque exécution |
| Exclure le jour en cours de gold | Gold ne contient que des jours finis | Le temps réel n'apparaît dans gold que le lendemain |

## Décision

**Le modèle silver `eco2mix_national` réunit les deux sources bronze :**

- l'historique, nettoyé comme avant (lignes artificielles du printemps écartées) ;
- le temps réel, réduit à la dernière version de chaque quart d'heure : `_source_updated_at` le plus récent, puis `_kafka_offset` le plus grand. La même règle d'heure locale le protège contre des lignes artificielles, qu'il n'a pas aujourd'hui ;
- **l'historique est prioritaire** : un quart d'heure présent dans l'historique écarte sa version temps réel. Les données consolidées, puis définitives, remplacent ainsi le temps réel au fil des publications de RTE ;
- une colonne `step_hours` (0,5 ou 0,25) donne la durée couverte par les valeurs réalisées de la ligne ;
- les quatre champs propres au temps réel deviennent des colonnes nullables (`wind_onshore_mw`, `wind_offshore_mw`, `battery_charging_mw`, `battery_discharging_mw`).

**Gold calcule l'énergie comme Σ puissance × `step_hours`.** `energy_mix_daily` compte les heures couvertes (`hours_with_data`) par rapport à la durée du jour (`expected_hours` : 23, 24 ou 25). `is_partial` signale le jour en cours (heure de Paris), et `renewable_share_monthly` le mois en cours.

**Le test des jours incomplets ignore le jour partiel**, et rien d'autre. Si le flux s'arrête, la veille devient incomplète sans être partielle : le test échoue, et le Write-Audit-Publish (ADR 008) ne publie pas un gold qui la compterait comme complète.

## Conséquences

- **Silver passe de 514 212 à 521 264 lignes** (7 052 quarts d'heure temps réel au 13 novembre), et gold va jusqu'au jour en cours.
- **L'historique est inchangé** : sur les 5 357 jours jusqu'au 31 août, l'écart de production entre l'ancien et le nouveau gold est de 0 MWh. Le 15 septembre, l'énergie recalculée à la main depuis le bronze (1 206 828 MWh) est celle de gold.
- **Hypothèse** : les valeurs temps réel sont traitées comme des puissances moyennes sur le quart d'heure, comme les valeurs consolidées. La spécification de RTE ne le précise que pour les données consolidées et définitives. Quand RTE consolidera septembre, l'écart entre les deux sources sur un même mois permettra de mesurer cette approximation.
- **Le schéma de gold change** : `expected_half_hours`, `half_hours_with_data` et `half_hours_with_detail` deviennent `expected_hours`, `hours_with_data` et `hours_with_detail`, en heures. Une requête qui lisait les anciennes colonnes doit être adaptée.
- **Une panne du flux temps réel bloque la publication de gold** jusqu'à ce que le producteur ait rattrapé son retard : c'est voulu, une journée incomplète ne doit pas paraître finie.
- Gold n'intègre le temps réel qu'au rythme du DAG dbt. Son rafraîchissement horaire fait l'objet de l'étape suivante.

## Sources

- RTE, « Spécification des fichiers de données en puissance pour éCO2mix », 1.1 : données consolidées et définitives en puissance moyenne, citée dans `transform/models/silver/_silver.yml`.
- Jeux ODRÉ `eco2mix-national-cons-def` et `eco2mix-national-tr`, décrits dans [docs/sources.md](../sources.md).
