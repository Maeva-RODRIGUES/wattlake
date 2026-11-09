# infra

Déploiement de la stack sur Kubernetes : configuration du cluster k3d, valeurs des charts Helm (SeaweedFS, Nessie, Trino, Airflow), manifestes de PostgreSQL (`postgres/`, kustomize) et Applications Argo CD (`argocd/`).

## GitOps avec Argo CD

Argo CD est le seul composant installé à la main (`make argocd-up`, chart `argo/argo-cd` 10.11.1, valeurs dans [argocd/values.yaml](argocd/values.yaml)). Il déploie tous les autres depuis la branche `main` du dépôt GitHub :

- [argocd/root.yaml](argocd/root.yaml) : l'Application racine (« app of apps »), appliquée par `make argocd-apps`. Elle déploie les Applications de [argocd/apps/](argocd/apps/).
- Une Application par composant : SeaweedFS, Nessie (avec le Secret de ses clés S3), Trino et Airflow combinent leur chart, à la version épinglée, et le fichier de valeurs de ce dossier. PostgreSQL déploie le dossier kustomize [postgres/](postgres/).

Le cluster suit donc ce qui est sur `main` : un changement de valeurs ou de version passe par un commit, pas par une commande `helm`.

## Reprise des composants installés par Helm

Avant Argo CD, les composants étaient installés par `helm upgrade --install` depuis le Makefile. Chaque Application garde le nom de la release Helm (`releaseName`), donc les mêmes noms d'objets : Argo CD reprend les objets existants au lieu d'en créer de nouveaux.

Avant la reprise, chaque chart a été rendu avec `helm template` puis comparé au cluster avec `kubectl diff`, en lecture seule. Résultat :

- **Nessie, Trino, SeaweedFS et PostgreSQL** : aucun objet en fonctionnement modifié. Seuls apparaissent les pods de test Helm (`*-test-connection`) et le Job qui crée le bucket `warehouse`, qui vérifie d'abord que le bucket existe. Le Deployment de Nessie est identique : la reprise ne le redémarre pas, ce qui compte tant que son catalogue est en mémoire (étape 4.5).
- **Airflow** : nouvelle image (ghcr.io, tag du commit) et nouveaux Jobs de migration et de création d'utilisateur. Les pods d'Airflow redémarrent, sans perte : les métadonnées sont dans PostgreSQL.

Les Applications sont d'abord en synchronisation manuelle, pour reprendre les composants un par un. Après la reprise, les métadonnées des anciennes releases Helm (secrets `sh.helm.release.v1.*`) sont supprimées : un `helm uninstall` effacerait sinon des objets désormais gérés par Argo CD.

## Airflow et Argo CD

Le chart Airflow demande, avec Argo CD, `useHelmHooks: false` et `applyCustomEnv: false` sur les Jobs de migration et de création d'utilisateur. Un Job ne se modifie pas une fois créé : l'annotation `argocd.argoproj.io/hook: Sync` fait recréer ces Jobs à chaque synchronisation au lieu de les modifier (voir [airflow/values.yaml](airflow/values.yaml)).
