# ADR 005 : plateforme d'intégration continue

- **Statut** : accepté
- **Date** : novembre 2026

## Contexte

Le dépôt est public et hébergé sur GitHub : c'est là que je développe, et là qu'un lecteur consulte le projet.

L'étape 4.2 met en place une intégration continue qui devra :

- vérifier le style du code (lint) ;
- lancer les tests Python de l'ingestion, qui n'ont besoin ni du réseau ni du cluster ;
- valider le projet dbt. `dbt parse` et `dbt compile` suffisent sans base de données, mais `dbt build` et les tests unitaires dbt demandent une connexion à Trino ;
- construire l'image Airflow, qui contient l'ingestion, dbt dans son propre environnement et les DAG, puis la publier dans un registre d'images.

Le projet a aussi deux contraintes. Les versions sont épinglées, et je n'adopte une version qu'au moins deux semaines après sa publication : un outil de mise à jour automatique des dépendances doit respecter ce délai. Ensuite, certaines dépendances transitives de l'ingestion, absentes des contraintes d'Airflow, ne sont pas figées (voir `transform/README.md`) : la CI doit pouvoir les surveiller.

Cet ADR choisit la plateforme. Le contenu exact des jobs est l'objet de l'étape 4.2.

## Critères

1. **Intégration avec l'hébergement** : statut des vérifications sur les pull requests, sans pièce supplémentaire à maintenir.
2. **Coût** pour un dépôt public.
3. **Registre d'images** pour publier l'image Airflow.
4. **Mises à jour des dépendances** compatibles avec la règle des deux semaines.
5. **Reproductibilité et sécurité** : actions ou images de CI épinglées, mêmes commandes en local et en CI.
6. **Capacités techniques** : construire une image Docker, lancer des conteneurs de service (un Trino de test par exemple).

## Options étudiées

| Option | Avantages | Limites |
| --- | --- | --- |
| GitHub Actions | Dans le dépôt, sans miroir. Gratuit pour un dépôt public sur les runners standard. Registre de paquets gratuit pour les paquets publics. Dependabot intégré, avec un délai minimal configurable. Conteneurs de service sur les runners Linux | Plateforme propre à GitHub. Les actions tierces sont du code externe : elles doivent être épinglées |
| GitLab CI, dépôt sur GitHub | Pipeline très complet, registre intégré, Renovate disponible | La CI pour un dépôt externe et le miroir entrant (GitHub vers GitLab) sont réservés aux offres Premium et Ultimate. Inaccessible en gratuit |
| GitLab CI, dépôt déplacé sur GitLab | Le miroir sortant (GitLab vers GitHub) est gratuit : le dépôt resterait visible sur GitHub | Changement d'hébergement principal. 400 minutes de calcul par mois en offre gratuite, la construction de l'image Airflow en consommerait une bonne part. Une pièce de plus (le miroir) entre le développement et la vitrine |
| Serveur CI auto-hébergé (Jenkins…) | Contrôle total | Un serveur de plus à maintenir et à sécuriser, sans bénéfice pour un projet public |

## Décision

**J'utilise GitHub Actions**, dans le dépôt existant, sur les runners Linux standard.

- **Registre** : l'image Airflow est publiée dans le registre de conteneurs de GitHub (`ghcr.io`), en paquet public.
- **Mises à jour des dépendances** : Dependabot, avec `cooldown.default-days: 14`, qui applique la règle des deux semaines aux mises à jour de version. Le délai ne s'applique pas aux mises à jour de sécurité, ce qui est souhaitable.
- **Épinglage** : chaque action tierce est épinglée par l'empreinte complète d'un commit (SHA), comme le recommande GitHub, avec la version lisible en commentaire.
- **Mêmes commandes qu'en local** : les jobs appellent les cibles du `Makefile` (`make ingestion-test`, `make airflow-image`…) plutôt que de dupliquer les commandes dans le fichier de workflow.

## Conséquences

- La CI se lit dans le dépôt lui-même : onglet Actions, statut des pull requests, badge dans le README.
- Dependabot ne connaît pas les versions épinglées dans le `Makefile` (charts Helm, version de Python) ni dans les fichiers de valeurs Helm. Il faudra les surveiller autrement : Renovate, dont l'option `minimumReleaseAge` joue le même rôle que `cooldown` et qui sait lire ces fichiers par expressions régulières, ou une revue manuelle. Le choix se fera à l'étape 4.2.
- Valider dbt jusqu'au `dbt build` en CI demanderait un Trino avec un catalogue Iceberg dans un conteneur de service. C'est possible sur les runners Linux, mais à évaluer à l'étape 4.2 face à une validation limitée à `dbt parse` et `dbt compile`.
- L'image Airflow construite en CI aura un tag tiré du commit, ce qui préparera le déploiement GitOps de l'étape 4.3.
- Je réexaminerai cette décision si le dépôt doit quitter GitHub, ou si les conditions de gratuité des runners ou du registre changent.

## Sources

- GitHub Actions, facturation : « The use of standard GitHub-hosted runners is free: In public repositories ». https://docs.github.com/en/billing/concepts/product-billing/github-actions
- GitHub Packages, facturation : « GitHub Packages usage is free for public packages ». https://docs.github.com/en/billing/concepts/product-billing/github-packages
- Dependabot, option `cooldown` : « Defines a cooldown period for dependency updates, allowing updates to be delayed for a configurable number of days. » https://docs.github.com/en/code-security/dependabot/working-with-dependabot/dependabot-options-reference
- GitHub, sécurité des workflows : « Pin actions to a full-length commit SHA ». https://docs.github.com/en/actions/security-for-github-actions/security-guides/security-hardening-for-github-actions
- GitHub Actions, conteneurs de service (runners Linux) : https://docs.github.com/en/actions/use-cases-and-examples/using-containerized-services/about-service-containers
- GitLab, CI pour un dépôt externe, « Tier: Premium, Ultimate » : https://docs.gitlab.com/ci/ci_cd_for_external_repos/
- GitLab, miroir entrant, « Tier: Premium, Ultimate » : https://docs.gitlab.com/user/project/repository/mirror/pull/
- GitLab, miroir sortant, « Tier: Free, Premium, Ultimate » : https://docs.gitlab.com/user/project/repository/mirror/push/
- GitLab, minutes de calcul : « Free tier namespaces receive 400 compute minutes per month. » https://docs.gitlab.com/ci/pipelines/compute_minutes/
- Renovate, `minimumReleaseAge` : « Time required before a new release is considered stable. » https://docs.renovatebot.com/configuration-options/#minimumreleaseage
