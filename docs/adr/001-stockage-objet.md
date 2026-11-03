# ADR 001 : choix du stockage objet

- **Statut** : accepté
- **Date** : novembre 2026

## Contexte

Iceberg a besoin d'un stockage objet compatible S3 pour héberger les fichiers de données et de métadonnées des tables. MinIO est le choix le plus répandu pour ce rôle en environnement local ou auto-hébergé.

En préparant le projet, j'ai constaté que :

- l'édition communautaire de MinIO n'est plus distribuée qu'en code source, sans binaires précompilés ;
- le dépôt officiel est archivé depuis avril 2026 ;
- les images publiques ne sont plus téléchargeables depuis septembre 2026, ce qui fait échouer une installation Helm standard.

## Critères

1. **Maintenance** : des versions et des correctifs de sécurité publiés régulièrement.
2. **Licence** : une licence permissive, sans obligation de republier le code modifié.
3. **Reproductibilité** : des images officielles, installables sans dépendre d'un tiers.
4. **Compatibilité** : l'API S3, pour rester transparent pour Trino et Iceberg.

## Options étudiées

| Option | Avantages | Limites |
| --- | --- | --- |
| Compiler MinIO ou utiliser un fork | Outil connu, nombreuses ressources | Dépendance à un tiers dont la pérennité est incertaine, licence AGPLv3 |
| AIStor Free (éditeur de MinIO) | Voie officielle | Licence propriétaire |
| SeaweedFS | Licence Apache 2.0, activement maintenu, chart Helm, API S3 | Moins répandu que MinIO, compatibilité S3 à valider sur mes usages |
| Garage | Léger, conçu pour l'auto-hébergement | Écosystème plus restreint |
| Ceph RGW | Standard des grandes infrastructures | Trop lourd pour un cluster local |

## Décision

J'utilise **SeaweedFS** comme stockage objet.

SeaweedFS propose aussi son propre catalogue Iceberg REST, mais je conserve **Nessie** comme catalogue : j'ai besoin de ses branches pour mettre en place le Write-Audit-Publish. Séparer le stockage du catalogue permet aussi de remplacer l'un sans toucher à l'autre, ce que ce changement a justement montré.

## Conséquences

- Trino et Iceberg ne voient que l'API S3 : revenir à MinIO, ou passer sur un stockage cloud, ne demanderait de modifier que la configuration.
- La compatibilité S3 de SeaweedFS doit être vérifiée sur les opérations utilisées par Iceberg.
- Sur une plateforme existante qui utilise déjà MinIO, je n'appliquerais pas cette décision telle quelle : je regarderais d'abord les versions déployées, le support souhaité et le coût d'une migration.

## Sources

- Dépôt officiel de MinIO (archivé) : https://github.com/minio/minio
- Retour d'expérience sur l'erreur 401 des images MinIO : https://jacar.es/?p=8616
- SeaweedFS : https://github.com/seaweedfs/seaweedfs
