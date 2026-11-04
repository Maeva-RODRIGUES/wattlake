# ADR 003 : base de métadonnées d'Airflow

- **Statut** : accepté
- **Date** : novembre 2026

## Contexte

Airflow stocke dans une base relationnelle l'état de l'orchestrateur : DAG, exécutions, tâches, connexions. Sa documentation réserve SQLite aux tests : « DO NOT use it in production ». Airflow 3.2.2 supporte PostgreSQL de la version 13 à la version 18.

Le chart Helm officiel d'Airflow (version 1.22.0) peut déployer cette base lui-même, avec le sous-chart PostgreSQL de Bitnami. En le préparant, j'ai constaté que :

- l'image référencée par défaut par ce sous-chart, `bitnami/postgresql:16.1.0-debian-11-r15`, n'existe plus sur Docker Hub ;
- le chart Airflow la remplace par `bitnamilegacy/postgresql:16.1.0-debian-11-r15`, toujours téléchargeable mais figée depuis juillet 2025 : PostgreSQL 16.1 sur Debian 11, sans mise à jour de sécurité ;
- l'image officielle `postgres`, elle, reçoit régulièrement de nouvelles versions correctives.

C'est la même situation que celle décrite dans l'ADR 001 pour MinIO : un composant qui s'installe encore, mais qui n'est plus maintenu.

## Critères

1. **Maintenance** : une image qui reçoit des correctifs de sécurité.
2. **Reproductibilité** : une image officielle, à une version épinglée.
3. **Simplicité** : peu de composants supplémentaires sur un cluster local.
4. **Réutilisation** : pouvoir accueillir aussi le stockage persistant de Nessie (étape 4.5).

## Options étudiées

| Option | Avantages | Limites |
| --- | --- | --- |
| PostgreSQL embarqué par le chart (`bitnamilegacy`) | Aucune configuration supplémentaire | Image figée, sans correctifs. Liée au cycle de vie d'Airflow, donc mal adaptée pour héberger d'autres bases |
| Image officielle `postgres`, déployée par mes propres manifestes | Image maintenue, version épinglée. Indépendante d'Airflow, réutilisable par Nessie | Manifestes à écrire et à maintenir. Sauvegardes et montées de version à ma charge |
| Opérateur CloudNativePG | Gestion de PostgreSQL par Kubernetes : sauvegardes, bascule, montées de version | Un opérateur et ses ressources personnalisées en plus, disproportionné pour une instance locale |
| SQLite | Aucun serveur | Réservé aux tests par la documentation d'Airflow |

## Décision

J'utilise l'**image officielle `postgres`, en version 17.10**, déployée en une seule instance par un StatefulSet avec un volume persistant, à partir de manifestes versionnés dans `infra/postgres/`. Airflow s'y connecte comme à une base externe, et son chart ne déploie pas PostgreSQL.

La version majeure 17 est supportée par Airflow 3.2.2 comme par la version 3.3, qui a abandonné la 13. Le tag 17.10 est le dernier publié depuis plus de deux semaines au moment du choix.

## Conséquences

- La base vit indépendamment d'Airflow : réinstaller Airflow ne touche pas aux métadonnées, et Nessie pourra y avoir sa propre base et son propre utilisateur à l'étape 4.5.
- Les montées de version de PostgreSQL et les sauvegardes sont à ma charge. Sur un cluster local, je n'en prévois pas.
- Les identifiants sont des valeurs de développement jusqu'à l'étape 4.4, comme pour les autres composants.
- En production, je réévaluerais CloudNativePG ou un service PostgreSQL managé, pour la haute disponibilité et les sauvegardes.

## Sources

- Prérequis d'Airflow 3.2.2 : https://airflow.apache.org/docs/apache-airflow/3.2.2/installation/prerequisites.html
- Chart Helm d'Airflow 1.22.0 : https://archive.apache.org/dist/airflow/helm-chart/1.22.0/airflow-1.22.0.tgz
- Image `bitnamilegacy/postgresql` : https://hub.docker.com/r/bitnamilegacy/postgresql
- Image officielle `postgres` : https://hub.docker.com/_/postgres
