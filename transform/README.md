# transform

Projet dbt (adaptateur Trino) qui construit les couches silver et gold à partir de la couche bronze.

## Utilisation

```bash
make dbt ARGS="debug"                        # configuration et connexion à Trino
make dbt ARGS="ls --resource-type source"    # sources déclarées
make dbt ARGS="build"                        # modèles et tests (à partir de l'étape 3.2)
```

La cible crée au besoin `transform/.venv` avec [uv](https://docs.astral.sh/uv/), en Python 3.13.13 comme l'image Airflow, à partir de `requirements.txt`. Elle lance ensuite dbt par [run.sh](run.sh), qui ouvre une redirection de port temporaire vers le service Trino du cluster (port local 18081), puis la referme à la fin, même en cas d'erreur.

- **Profil versionné** : [profiles.yml](profiles.yml) est dans le projet, passé à dbt par `--profiles-dir`, plutôt que dans `~/.dbt`. L'hôte et le port viennent des variables `TRINO_HOST` et `TRINO_PORT`, ce qui permettra d'utiliser le même profil depuis Airflow (étape 3.5).
- **Source** : la table bronze est déclarée dans [models/bronze/sources.yml](models/bronze/sources.yml). Ses particularités, dont les 60 lignes artificielles du passage à l'heure d'été, sont décrites dans [docs/sources.md](../docs/sources.md).
- **Statistiques d'usage désactivées** (`send_anonymous_usage_stats: false`), et vérification des certificats activée dès maintenant (`require_certificate_validation: true`). `dbt debug` affiche encore l'avertissement sur les certificats, car il teste la connexion avant de lire les réglages du projet. Les autres commandes ne l'affichent plus.

## Exécution par Airflow

Dans le cluster, le DAG `dbt_transformations` lance `dbt build` chaque fois que la table bronze change (voir [ingestion/README.md](../ingestion/README.md)). L'image Airflow contient :

- un environnement virtuel dbt dédié, `/opt/dbt/venv`, installé depuis [requirements.txt](requirements.txt), séparé de celui d'Airflow dont les contraintes diffèrent pour 26 paquets communs ;
- ce projet, dans `/opt/wattlake/transform`, avec `dbt_utils` déjà installé par `dbt deps` à la construction de l'image.

Une modification du projet dbt n'arrive dans Airflow qu'avec une nouvelle image : la CI la publie pour chaque commit sur `main`, puis son SHA est reporté dans `images.airflow.tag` de `infra/airflow/values.yaml`, qu'Argo CD déploie (voir « Déployer une modification » dans le [README](../README.md)).

## Intégration continue

La CI lance `make dbt-parse` : `dbt deps` puis `dbt parse`, sans base de données. Cela valide le YAML, le Jinja, les références entre modèles et la définition des tests. `dbt compile`, `dbt build` et les tests unitaires dbt ouvrent une connexion à Trino : ils tournent dans le cluster, pas en CI.

## Versions

| Paquet | Version | Raison |
| --- | --- | --- |
| `dbt-core` | 1.12.6 | Seule version 1.x en support actif, jusqu'au 15 juillet 2027. La 1.10 est dépréciée, et la 1.11 est en support critique jusqu'au 18 décembre 2026 |
| `dbt-trino` | 1.10.6 | Dernière version publiée depuis plus de deux semaines. La 1.11.0 date du 26 octobre 2026 |

**Installation dépendante de github.com** : dbt-core 1.12 dépend de `dbt-core-experimental-parser` (2.0.8), publié sur PyPI uniquement sous forme de sources. Son installation télécharge un binaire depuis les releases GitHub de dbt-labs. Une installation de dbt, et donc la construction de l'image Airflow, échoue si github.com ne répond pas (constaté une fois : délai dépassé, réussite au deuxième essai).

Depuis dbt 1.8, dbt-core et les adaptateurs sont publiés séparément et communiquent par le paquet `dbt-adapters` : leurs versions mineures n'ont plus à correspondre. dbt-trino 1.10.6 déclare accepter `dbt-core>=1.8,<2.0`.

**Validation de la combinaison** : dbt-core 1.12 et dbt-trino 1.10 n'ont pas forcément été testés ensemble par leurs mainteneurs. À l'étape 3.1, `dbt debug`, `dbt parse`, `dbt ls` et `dbt show` ont validé la connexion et l'exécution de requêtes. À l'étape 3.2, le premier `dbt build` a validé la matérialisation, le 6 novembre 2026 :

- création du schéma `iceberg.silver` et de la table `eco2mix_national` (514 212 lignes, en 5 secondes), puis exécution des tests `unique` et `not_null` ;
- second `dbt build` : table remplacée par un `create or replace table` atomique, avec le même résultat, en un seul commit Nessie.

## Couche silver

Le modèle [models/silver/eco2mix_national.sql](models/silver/eco2mix_national.sql) produit `iceberg.silver.eco2mix_national` : une ligne par quart d'heure, typée et dédoublonnée.

- **Dédoublonnage** : une ligne n'est gardée que si son heure locale (`heure`) correspond à `date_heure` en heure de Paris. La règle écarte exactement les 60 lignes artificielles du passage à l'heure d'été, et `timestamp_utc` devient unique.
- **Typage** : `ND` (« non disponible ») devient `NULL`, puis les nombres publiés en texte sont convertis en entier. Une autre valeur non numérique fait échouer le modèle. `date` et `heure` sont remplacées par `local_date`, de type `date`.
- **Noms en anglais, avec l'unité** (`consumption_mw`, `co2_intensity_g_per_kwh`…). Le nom d'origine de chaque colonne est dans son `meta.source_field`.
- **Matérialisation** : table reconstruite en entier à chaque exécution, avec `on_table_exists: replace`. Un modèle incrémental ne se justifie pas pour environ 500 000 lignes.

**Puissance et énergie** : les colonnes `*_mw` sont des puissances moyennes sur le pas de temps, pas des énergies. [models/silver/_silver.yml](models/silver/_silver.yml) donne, pour chaque colonne, son pas de publication et sa conversion en MWh :

| Colonnes | Pas | Énergie (MWh) |
| --- | --- | --- |
| Consommation, filières, pompage, échanges physiques | 30 min (lignes :00 et :30) | valeur × 0,5 |
| Détail par technologie | 30 min, à partir de 2013 | valeur × 0,5 |
| Prévisions de consommation | 15 min | valeur × 0,25 |
| Échanges commerciaux | 30 min, puis 15 min à partir du 2024-06-26 22:00 UTC | valeur × 0,5, puis × 0,25 |
| `co2_intensity_g_per_kwh` | 30 min | Pas une puissance : moyenne pondérée par l'énergie produite |

Deux hypothèses, que la spécification de RTE ne tranche pas : `timestamp_utc` marque le début de l'intervalle, et les échanges commerciaux sont des puissances moyennes, comme la production et la consommation.

## Couche gold

Deux tables dans `iceberg.gold`, documentées dans [models/gold/_gold.yml](models/gold/_gold.yml) :

- **`energy_mix_daily`** : une ligne par jour en heure de Paris, avec l'énergie en MWh de chaque filière (puissance × 0,5 h sur les demi-heures), la production totale des huit filières sans le pompage, et les parts de renouvelables. Elle indique aussi la complétude du jour : 46, 48 ou 50 demi-heures attendues selon les changements d'heure, et `is_complete`.
- **`renewable_share_monthly`** : une ligne par mois, avec les sommes des énergies quotidiennes et les parts recalculées à partir de ces sommes.

Points de conception :

- **Ratio de sommes, pas moyenne de parts** : une part sur une période vaut somme des énergies renouvelables / somme de la production, ce qui pondère chaque pas de temps par son énergie. Sur 2024, la moyenne des parts quotidiennes surestime la part de 0,12 point (0,2928 contre 0,2916).
- **Deux définitions** : `renewable_share`, au sens large (éolien, solaire, hydraulique, bioénergies), et `renewable_share_strict`, selon les conventions du SDES (sans le turbinage des STEP, avec 50 % des déchets). La part stricte vaut `NULL` en 2012, et non 0, car le détail par technologie n'est publié qu'à partir de 2013.
- **Schéma `gold`** : la macro [macros/generate_schema_name.sql](macros/generate_schema_name.sql) utilise le nom de schéma tel quel. Sans elle, dbt aurait créé `silver_gold`.

Contrôle croisé avec le Bilan électrique 2024 de RTE (chiffres provisionnels) : production 537,6 TWh contre 536,5, renouvelables au sens strict 148,0 TWh contre 148, éolien 46,8 contre 46,6. Le solaire (24,5 TWh contre 23,3) reste un écart de 5 % non expliqué.

## Tests

`make dbt ARGS="build"` construit chaque modèle puis lance ses tests : un test en échec arrête les modèles qui en dépendent.

- **Paquet `dbt_utils` 1.4.1**, déclaré dans [packages.yml](packages.yml) et figé dans [package-lock.yml](package-lock.yml). Il s'installe avec `make dbt ARGS="deps"`, dans `dbt_packages/` (ignoré par Git). À l'étape 3.5, il faudra l'installer au moment de construire l'image, pas à chaque exécution.
- **Tests de données** (37), dans `_silver.yml` et `_gold.yml`, tous en sévérité `error`. Chaque seuil vient d'une mesure sur les données, rappelée en commentaire : pompage toujours ≤ 0, production ≥ 0 sauf solaire, charbon et turbinage des STEP (≥ -200 MW, consommations auxiliaires jusqu'en 2020), consommation entre 20 000 et 120 000 MW, intensité CO₂ entre 0 et 500 g/kWh, parts entre 0 et 1, part stricte ≤ part large et nulle exactement avant 2013, aucun jour manquant, production égale à la somme des huit filières.
- **Tests SQL dédiés**, dans [tests/](tests/) : les journées incomplètes se limitent aux cas connus, et les sommes mensuelles égalent celles des jours.
- **Tests unitaires** (2) : le SQL des modèles exécuté sur des lignes simulées, sans lire les tables. L'un vérifie qu'une ligne artificielle de mars est écartée et que `ND` devient `NULL`, l'autre que les parts sont des ratios de sommes (0,6 et non la moyenne 0,54) et que la part stricte vaut `NULL` sans détail par technologie. Pour vérifier qu'ils détectent une régression, on a retiré le filtre de dédoublonnage, puis forcé la part stricte inconnue à 0 : chaque erreur a fait échouer le test prévu.

Non testé volontairement : l'égalité entre une filière et la somme de ses technologies, que la source ne respecte pas (particularité 9 de [docs/sources.md](../docs/sources.md)).

Journées incomplètes : chaque dimanche de passage à l'heure d'hiver (la source omet l'une des deux heures répétées, voir la particularité 8 de [docs/sources.md](../docs/sources.md)) et le 1er janvier 2026.

## Dépendances : pourquoi `requirements.in` et `uv pip compile`

- [requirements.in](requirements.in) liste les deux dépendances directes.
- [requirements.txt](requirements.txt), généré par `make transform-lock` (`uv pip compile`), fige toutes les dépendances transitives, une soixantaine de paquets. La commande exacte est inscrite en tête du fichier.
- `--exclude-newer`, réglé sur la date du jour moins 14 jours, ignore les versions plus récentes : chaque dépendance, même transitive, a été publiée depuis au moins deux semaines.
- L'environnement est installé par `uv pip sync`, qui reproduit exactement le fichier et retire tout paquet en trop.

Le module d'ingestion utilise un autre mécanisme : un `pyproject.toml` qui épingle ses dépendances directes, installé avec le fichier de contraintes d'Airflow 3.2.2. La différence est voulue :

- **La référence des versions n'est pas la même.** Le paquet d'ingestion s'installe dans l'image Airflow : ses dépendances transitives doivent être celles d'Airflow, et le fichier de contraintes d'Airflow fait foi. dbt n'est pas couvert par ces contraintes, et ses dépendances risquent même d'entrer en conflit avec elles. Il lui faut son propre fichier de versions figées.
- **`transform/` n'est pas un paquet Python.** C'est un projet dbt, sans code à construire. Un `requirements.txt` s'installe avec n'importe quel `pip`, y compris dans une image sans uv (étape 3.5). Un `uv.lock` demanderait uv ou une conversion.

Limite connue de l'ingestion : les dépendances transitives absentes des contraintes d'Airflow, comme celles de PyIceberg (`strictyaml`, `pyroaring`, `mmh3`), n'y sont pas figées. C'est un point à traiter avec l'intégration continue (étape 4.2).
