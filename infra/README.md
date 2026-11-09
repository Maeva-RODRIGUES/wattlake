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

La reprise s'est faite en synchronisation manuelle, composant par composant. L'écart calculé par Argo CD a confirmé la mesure faite avec `kubectl diff` : pour PostgreSQL, SeaweedFS, Trino et Nessie, la seule différence était l'annotation de suivi d'Argo CD (`argocd.argoproj.io/tracking-id`), et aucun pod n'a redémarré. Les métadonnées des anciennes releases Helm (secrets `sh.helm.release.v1.*`) ont ensuite été supprimées : un `helm uninstall` aurait sinon effacé des objets désormais gérés par Argo CD.

## Synchronisation automatique

Toutes les Applications sont en synchronisation automatique, avec `prune` (ce qui disparaît de Git est supprimé du cluster) et `selfHeal` (un changement fait à la main dans le cluster est annulé). Argo CD vérifie le dépôt toutes les trois minutes environ.

L'Application racine elle-même n'est pas lue depuis Git : elle est appliquée par `make argocd-apps`. Une modification de [argocd/root.yaml](argocd/root.yaml) se déploie en relançant cette cible ; une modification des Applications de [argocd/apps/](argocd/apps/) se déploie toute seule.

## Airflow et Argo CD

Le chart Airflow demande, avec Argo CD, `useHelmHooks: false` et `applyCustomEnv: false` sur les Jobs de migration et de création d'utilisateur. Un Job ne se modifie pas une fois créé : l'annotation `argocd.argoproj.io/hook: Sync` fait recréer ces Jobs à chaque synchronisation au lieu de les modifier (voir [airflow/values.yaml](airflow/values.yaml)).
