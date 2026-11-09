# ADR 006 : gestion des secrets

- **Statut** : accepté
- **Date** : novembre 2026

## Contexte

Jusqu'à l'étape 4.3, les identifiants de la stack étaient des valeurs de développement écrites en clair dans le dépôt :

- les clés S3 de SeaweedFS, partagées par Trino, Nessie et Airflow sous une même identité `admin` ;
- les mots de passe PostgreSQL (superutilisateur et rôle `airflow`) ;
- les clés d'Airflow (Fernet, API, JWT) et le mot de passe de son interface.

Depuis l'étape 4.3, Argo CD déploie la stack depuis la branche `main` : Git est la source de vérité du cluster. Or un Secret Kubernetes n'est qu'un encodage base64, sans chiffrement. Écrit dans Git, même dans un fichier YAML de type `Secret`, il est lisible par quiconque lit le dépôt, ici public.

La question est donc : comment fournir des secrets à un cluster piloté par Git, sans les mettre en clair dans Git ?

## Critères

1. **Aucun secret lisible dans le dépôt**, ni dans son historique.
2. **Compatible avec Argo CD** et sa synchronisation automatique avec `prune`.
3. **Simplicité en local** : le cluster k3d est détruit et recréé par `make down` et `make up`.
4. **Peu de pièces en plus** à installer, à mettre à jour et à expliquer.
5. **Chemin vers la production** : la solution locale ne doit pas imposer un modèle qui serait à refaire.

## Options étudiées

| Option | Principe | Avantages | Limites |
| --- | --- | --- | --- |
| A. Secrets générés hors Git | Un script crée les Secrets avec des valeurs aléatoires. Git ne contient que leurs noms | Aucun composant ni clé de chiffrement en plus. Les manifestes ne référencent que des noms de Secrets, comme avec External Secrets | Les valeurs ne sont pas versionnées, et le cluster n'est pas entièrement décrit par Git : une étape (`make secrets`) précède Argo CD |
| B. Sealed Secrets | Les Secrets sont chiffrés avec la clé publique d'un contrôleur, puis commités. Le contrôleur les déchiffre dans le cluster | Tout est dans Git, chiffré. Outil très répandu | La clé privée est générée dans le cluster : `make down` la détruit, et les fichiers commités deviennent indéchiffrables. Il faudrait la sauvegarder hors du dépôt et la restaurer à chaque `make up` |
| C. SOPS et age | Les fichiers sont chiffrés avec une clé age gardée sur le poste, puis déchiffrés au rendu par un plugin d'Argo CD (KSOPS) | Tout est dans Git, chiffré. La clé survit à la recréation du cluster | Le repo-server d'Argo CD doit embarquer le plugin et la clé : c'est la configuration la plus délicate, et la clé du poste devient une pièce critique |
| D. External Secrets et un coffre | Un opérateur lit les secrets dans un coffre (Vault, gestionnaire de secrets d'un cloud) et crée les Secrets Kubernetes | Solution de production : rotation, audit, droits d'accès gérés par le coffre | Lourd en local. Un Vault dans le cluster pose lui-même la question de son initialisation et de son déverrouillage |

## Décision

**J'utilise l'option A.** La cible `make secrets` lance [infra/secrets/create.sh](../../infra/secrets/create.sh), qui crée cinq Secrets dans le namespace `wattlake`, avec des valeurs aléatoires tirées par `openssl rand` :

| Secret | Contenu | Utilisé par |
| --- | --- | --- |
| `seaweedfs-s3-config` | Configuration des identités S3 (JSON), et les clés de l'identité `admin` | Passerelle S3 de SeaweedFS |
| `nessie-credentials` | Clés S3 de l'identité `nessie` | Nessie |
| `trino-credentials` | Clés S3 de l'identité `trino` | Trino |
| `postgres-credentials` | Mots de passe du superutilisateur et du rôle `airflow` | PostgreSQL (à l'initialisation) |
| `airflow-credentials` | URL de connexion à la base, clés Fernet, API et JWT, mot de passe admin, clés S3 de l'identité `airflow` | Airflow |

- **Une identité S3 par composant.** `nessie`, `trino` et `airflow` ne peuvent que lire, écrire et lister le bucket `warehouse`. Une clé qui fuit ne donne accès qu'à ce bucket, et se change sans toucher aux autres composants. L'identité `admin` n'est utilisée par aucun composant : elle sert à l'exploitation.
- **Tout ou rien.** Plusieurs Secrets partagent une valeur : chaque paire de clés S3 figure aussi dans la configuration de SeaweedFS, et le mot de passe du rôle `airflow` dans l'URL de connexion d'Airflow. Le script crée donc les cinq Secrets ensemble, ne remplace jamais un Secret existant, et s'arrête si seuls certains existent.
- **Des noms que les charts ne génèrent pas.** Argo CD supprime (`prune`) les objets qu'il suit et qui disparaissent du rendu. Les Secrets de `make secrets` portent donc des noms qu'aucun chart ne produit : ils ne sont pas suivis par Argo CD, et ne peuvent pas être supprimés par lui.
- **Les valeurs ne quittent pas le cluster.** Le script envoie les manifestes sur l'entrée standard de `kubectl`, jamais en argument de commande. Les pods lisent les valeurs par `secretKeyRef`.

## Conséquences

- `make up` enchaîne maintenant `cluster-up`, `secrets`, `argocd-up` et `argocd-apps`. Sur un cluster neuf, les Secrets existent avant le premier déploiement d'Argo CD.
- **Changer une valeur** se fait à la main, et dépend du composant :
  - les clés S3 et les clés d'Airflow : supprimer les cinq Secrets, relancer `make secrets`, puis redémarrer les pods ;
  - PostgreSQL ne lit ses mots de passe qu'à l'initialisation d'un volume vide : sur un volume existant, il faut aussi un `ALTER ROLE` (voir [infra/README.md](../../infra/README.md)) ;
  - changer la clé Fernet rend illisibles les connexions et variables chiffrées déjà stockées par Airflow.
- Les Secrets restent stockés sans chiffrement dans la base du cluster (etcd, ou SQLite pour k3s). Le chiffrement au repos ne protège pas des droits de lecture sur les Secrets dans le namespace : il n'est pas activé sur ce cluster local.
- **En production**, je choisirais l'option D, avec le gestionnaire de secrets du cloud utilisé. Les manifestes du dépôt ne changeraient pas : ils référencent des noms de Secrets, que créerait l'opérateur External Secrets au lieu du script.
- Je réexaminerai cette décision si la stack doit tourner ailleurs que sur un poste, ou si plusieurs personnes doivent partager les mêmes valeurs.

## Sources

- Kubernetes, Secrets : les Secrets sont stockés sans chiffrement par défaut dans etcd. https://kubernetes.io/docs/concepts/configuration/secret/
- Argo CD, gestion des secrets : approches recommandées et outils. https://argo-cd.readthedocs.io/en/stable/operator-manual/secret-management/
- Argo CD, options de synchronisation (`prune`). https://argo-cd.readthedocs.io/en/stable/user-guide/auto_sync/
- Sealed Secrets : https://github.com/bitnami-labs/sealed-secrets
- SOPS : https://github.com/getsops/sops ; KSOPS : https://github.com/viaduct-ai/kustomize-sops
- External Secrets Operator : https://external-secrets.io/
- SeaweedFS, identités et actions limitées à un bucket (`Read:bucket1`, `Write:bucket1`, `List:bucket1`) : https://github.com/seaweedfs/seaweedfs/wiki/S3-Credentials
- Chart Airflow, secrets fournis par l'utilisateur (`fernetKeySecretName`, `data.metadataSecretName`…) : https://airflow.apache.org/docs/helm-chart/stable/production-guide.html
