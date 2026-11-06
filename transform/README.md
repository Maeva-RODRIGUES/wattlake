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

## Versions

| Paquet | Version | Raison |
| --- | --- | --- |
| `dbt-core` | 1.12.6 | Seule version 1.x en support actif, jusqu'au 15 juillet 2027. La 1.10 est dépréciée, et la 1.11 est en support critique jusqu'au 18 décembre 2026 |
| `dbt-trino` | 1.10.6 | Dernière version publiée depuis plus de deux semaines. La 1.11.0 date du 26 octobre 2026 |

Depuis dbt 1.8, dbt-core et les adaptateurs sont publiés séparément et communiquent par le paquet `dbt-adapters` : leurs versions mineures n'ont plus à correspondre. dbt-trino 1.10.6 déclare accepter `dbt-core>=1.8,<2.0`.

**Combinaison à valider** : dbt-core 1.12 et dbt-trino 1.10 n'ont pas forcément été testés ensemble par leurs mainteneurs. Pour l'instant, la combinaison n'est vérifiée que par `dbt debug`, `dbt parse`, `dbt ls` et une lecture de la source avec `dbt show`. Ces commandes valident la connexion et l'exécution de requêtes, mais pas la matérialisation : création et remplacement de tables Iceberg, exécution des tests. Le premier `dbt build` de l'étape 3.2, sur les modèles silver, sert de validation de la matérialisation. En cas d'échec lié à l'adaptateur, il faudra revoir ces versions avant d'aller plus loin.

## Dépendances : pourquoi `requirements.in` et `uv pip compile`

- [requirements.in](requirements.in) liste les deux dépendances directes.
- [requirements.txt](requirements.txt), généré par `make transform-lock` (`uv pip compile`), fige toutes les dépendances transitives, une soixantaine de paquets. La commande exacte est inscrite en tête du fichier.
- `--exclude-newer`, réglé sur la date du jour moins 14 jours, ignore les versions plus récentes : chaque dépendance, même transitive, a été publiée depuis au moins deux semaines.
- L'environnement est installé par `uv pip sync`, qui reproduit exactement le fichier et retire tout paquet en trop.

Le module d'ingestion utilise un autre mécanisme : un `pyproject.toml` qui épingle ses dépendances directes, installé avec le fichier de contraintes d'Airflow 3.2.2. La différence est voulue :

- **La référence des versions n'est pas la même.** Le paquet d'ingestion s'installe dans l'image Airflow : ses dépendances transitives doivent être celles d'Airflow, et le fichier de contraintes d'Airflow fait foi. dbt n'est pas couvert par ces contraintes, et ses dépendances risquent même d'entrer en conflit avec elles. Il lui faut son propre fichier de versions figées.
- **`transform/` n'est pas un paquet Python.** C'est un projet dbt, sans code à construire. Un `requirements.txt` s'installe avec n'importe quel `pip`, y compris dans une image sans uv (étape 3.5). Un `uv.lock` demanderait uv ou une conversion.

Limite connue de l'ingestion : les dépendances transitives absentes des contraintes d'Airflow, comme celles de PyIceberg (`strictyaml`, `pyroaring`, `mmh3`), n'y sont pas figées. C'est un point à traiter avec l'intégration continue (étape 4.2).
