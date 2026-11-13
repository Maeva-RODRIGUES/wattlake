# ADR 014 : DNS de secours pour les noms externes

- **Statut** : accepté
- **Date** : novembre 2026

## Contexte

Les nœuds k3d sont des conteneurs Docker. Leur résolveur, le DNS intégré de Docker, garde les serveurs DNS de l'ordinateur tels qu'ils étaient au démarrage du conteneur. Après un changement de réseau, il interroge encore ceux de l'ancien réseau et renvoie SERVFAIL. CoreDNS, qui transmet les noms externes à ce résolveur, ne résout plus rien hors du cluster. Constaté les 9, 10, 11, 12 et 13 novembre 2026 :

- **Argo CD** ne joint plus GitHub : les Applications passent en `Unknown` ;
- **le producteur temps réel** ne joint plus l'API ODRÉ : il saute ses cycles ;
- depuis l'ADR 012, **la publication de gold** est bloquée, car la veille devient incomplète.

Jusqu'ici, la seule correction était `make cluster-restart`, à la main. Deux corrections permanentes avaient été écartées dans `infra/README.md` : des DNS publics fixes dans CoreDNS (bloqués sur les réseaux d'entreprise, et toutes les requêtes chez un tiers), et faire écouter systemd-resolved sur l'interface Docker (configuration de l'ordinateur, hors du dépôt).

Ce que permet la stack, vérifié sur le cluster et dans le code de CoreDNS 1.14.3 :

- Le Corefile de k3s contient `forward . /etc/resolv.conf`, et k3s le réécrit à chaque démarrage : le modifier ne tiendrait pas. Il importe en revanche les fichiers d'un ConfigMap facultatif, `coredns-custom` (`import /etc/coredns/custom/*.server` pour des blocs de serveur supplémentaires).
- Le plugin `forward` accepte plusieurs serveurs : `policy sequential` les essaie dans l'ordre, et `failover SERVFAIL REFUSED` passe au suivant aussi sur ces réponses, en plus des délais dépassés.

## Critères

1. **Le cluster continue de résoudre les noms dont il a besoin** après un changement de réseau.
2. **Pas de requête chez un tiers** tant que le DNS local fonctionne.
3. **Pas de régression** sur un réseau qui bloque le DNS sortant.
4. **Déclaratif, sans recréer le cluster** (qui perdrait ses données).

## Options étudiées

| Option | Avantages | Limites |
| --- | --- | --- |
| Blocs de serveur par zone dans `coredns-custom` : DNS local, puis DNS publics en cas d'échec | En GitOps, sans recréer le cluster. Les DNS publics ne servent qu'en secours | Ne couvre que les zones listées. Ne corrige pas le téléchargement des images |
| Remplacer le CoreDNS de k3s (`--disable coredns`) par le nôtre | Corefile complet sous contrôle | Recréer le cluster, donc perdre les données |
| Ajouter des serveurs au kubelet (`--resolv-conf`) | Tous les noms couverts | Recréer le cluster. Répartition au hasard entre serveurs, pas en secours |
| DNS publics en premier | Plus simple | Toutes les requêtes externes chez un tiers |

## Décision

**Un ConfigMap `coredns-custom`**, déployé par l'Application Argo CD `coredns` (`infra/coredns/`), ajoute un bloc de serveur pour les zones externes de la stack : `github.com`, `github.io`, `githubusercontent.com` (Argo CD et la plupart des charts), `opendatasoft.com` (producteur), `strimzi.io`, `apache.org` et `projectnessie.org` (autres charts). Ce bloc passe avant le bloc général `.` pour ces zones.

Il interroge `/etc/resolv.conf` (le résolveur du nœud), puis 1.1.1.1, puis 9.9.9.9, dans cet ordre (`policy sequential`), en passant au suivant sur un délai dépassé, SERVFAIL ou REFUSED (`failover`).

## Conséquences

- **Après un changement de réseau, Argo CD, les charts et le producteur continuent de résoudre leurs noms** par les DNS publics, sans redémarrage.
- **Testé** : une zone d'essai dont le premier serveur ne répond jamais (`192.0.2.1`) s'est résolue en 2,1 s par le serveur suivant. Les zones couvertes se résolvent en 50 à 120 ms par le DNS local. CoreDNS a rechargé la configuration sans redémarrage (plugin `reload`).
- **Ne couvre pas le téléchargement des images** : containerd passe par le résolveur du nœud, que CoreDNS ne remplace pas. Un nouveau déploiement d'image reste bloqué jusqu'à `make cluster-restart` ; `make cluster-dns-check` le détecte toujours.
- **Une nouvelle dépendance externe** (un nouveau chart sur un autre domaine) doit être ajoutée à la liste des zones.
- **Hors des pannes, aucune requête ne part chez Cloudflare ou Quad9.** Sur un réseau qui bloque le DNS sortant, les DNS publics échouent aussi : le comportement est celui d'avant.

## Sources

- CoreDNS 1.14.3, plugin `forward` (`policy`, `failover`). https://github.com/coredns/coredns/blob/v1.14.3/plugin/forward/README.md
- k3s v1.35.5+k3s1, manifeste de CoreDNS (imports `/etc/coredns/custom/*.override` et `*.server`, ConfigMap facultatif `coredns-custom`). https://github.com/k3s-io/k3s/blob/v1.35.5%2Bk3s1/manifests/coredns.yaml
