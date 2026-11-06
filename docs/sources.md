# Sources de données

WattLake utilise les données éCO2mix de RTE, le gestionnaire du réseau de transport d'électricité français. Elles décrivent, au pas quart d'heure ou demi-heure, la consommation d'électricité en France, sa production par filière, les échanges aux frontières et les émissions de CO₂ associées.

Toutes les informations de ce document ont été vérifiées sur le portail et son API le 4 novembre 2026. Les chiffres de volume et de couverture évoluent à chaque mise à jour.

## Fournisseur et portail

- **Producteur** : RTE (Réseau de transport d'électricité).
- **Portail** : [ODRÉ](https://odre.opendatasoft.com/), Open Data Réseaux Énergies, qui publie les données de plusieurs gestionnaires de réseaux.
- **API** : Opendatasoft Explore API v2.1, publique, sans authentification : `https://odre.opendatasoft.com/api/explore/v2.1/`.

## Jeux de données retenus

Le portail publie cinq jeux éCO2mix : national et régional, chacun en version temps réel et en version consolidée et définitive, plus un jeu temps réel par métropole. WattLake retient les deux jeux **nationaux**, suffisants pour suivre la part des énergies renouvelables dans le mix. Les jeux régionaux (`eco2mix-regional-cons-def`, `eco2mix-regional-tr`) et métropoles (`eco2mix-metropoles-tr`) sont des extensions possibles.

| | Historique | Temps réel |
| --- | --- | --- |
| Identifiant | [`eco2mix-national-cons-def`](https://odre.opendatasoft.com/explore/dataset/eco2mix-national-cons-def/) | [`eco2mix-national-tr`](https://odre.opendatasoft.com/explore/dataset/eco2mix-national-tr/) |
| Rôle dans WattLake | Chargement batch de l'historique dans bronze (étape 2) | Flux temps réel (étape 5) |
| Couverture | Du 1ᵉʳ janvier 2012 au 31 août 2026 | Du 1ᵉʳ septembre 2026 au lendemain (J+1) |
| Volume | 514 272 lignes | 6 336 lignes, fenêtre glissante |
| Rafraîchissement | Une fois par jour | Tous les quarts d'heure |
| Nature des données | Définitives jusqu'au 31 décembre 2025, consolidées depuis le 1ᵉʳ janvier 2026 | Temps réel |

Les dates de couverture sont exprimées en heure de Paris.

### Nature des données

Une même période passe par trois états successifs, indiqués par le champ `nature` :

1. **Temps réel** : télémesures des ouvrages, complétées par des forfaits et des estimations.
2. **Consolidées** : données vérifiées et complétées, livrées vers le milieu du mois suivant.
3. **Définitives** : tous les partenaires ont transmis et vérifié leurs comptages, livraison au second semestre de l'année suivante.

Une valeur déjà chargée peut donc changer quand elle passe d'un état au suivant. Le chargement doit pouvoir réécrire une période.

Les deux jeux se suivent exactement : le temps réel commence au quart d'heure qui suit la dernière donnée consolidée. À la date de vérification, historique et temps réel se raccordent sans trou ni chevauchement.

## Champs

Les deux jeux partagent l'essentiel de leurs champs. Les puissances sont en mégawatts (MW).

| Thème | Champs | Remarques |
| --- | --- | --- |
| Horodatage | `date_heure` | Horodatage en UTC, à utiliser comme référence |
| | `date`, `heure` | Texte en heure locale, ambigu aux changements d'heure |
| Contexte | `perimetre`, `nature` | `perimetre` vaut toujours `France` |
| Consommation | `consommation` | Consommation réalisée |
| | `prevision_j1`, `prevision_j` | Prévisions établies la veille et le jour même |
| Production par filière | `nucleaire`, `eolien`, `solaire`, `hydraulique`, `bioenergies`, `gaz`, `charbon`, `fioul` | |
| Détail par technologie | `fioul_tac`, `fioul_cogen`, `fioul_autres`, `gaz_tac`, `gaz_cogen`, `gaz_ccg`, `gaz_autres`, `hydraulique_fil_eau_eclusee`, `hydraulique_lacs`, `hydraulique_step_turbinage`, `bioenergies_dechets`, `bioenergies_biomasse`, `bioenergies_biogaz` | Sous-totaux de chaque filière |
| Stockage | `pompage` | Pompes des STEP (stations de transfert d'énergie par pompage) |
| | `stockage_batterie`, `destockage_batterie` | Temps réel uniquement |
| Échanges | `ech_physiques` | Solde des flux physiques aux frontières |
| | `ech_comm_angleterre`, `ech_comm_espagne`, `ech_comm_italie`, `ech_comm_suisse`, `ech_comm_allemagne_belgique` | Solde des échanges commerciaux par frontière |
| Émissions | `taux_co2` | Estimation en grammes de CO₂ par kWh produit |

Champs présents uniquement dans le temps réel : `eolien_terrestre`, `eolien_offshore`, `stockage_batterie`, `destockage_batterie`.

### Conventions de signe

- **Échanges** : une valeur négative signifie que la France exporte, une valeur positive qu'elle importe (définition du portail).
- **Pompage et recharge des batteries** : stockés en **valeurs négatives**, car ce sont des consommations. Le pompage varie de -4 086 à -1 MW dans l'historique, la recharge des batteries de -487 à 0 MW dans le temps réel.
- **Déstockage des batteries** : valeurs positives, de 0 à 544 MW dans le temps réel.

Ces signes doivent être pris en compte pour additionner les filières : ajouter le pompage à la production la diminuerait.

## Particularités à traiter

Ces points concernent surtout la couche silver.

1. **Deux pas de temps dans l'historique** : les prévisions sont au quart d'heure, mais la consommation réalisée, la production, les échanges et le CO₂ sont à la demi-heure. Les lignes de :15 et :45 n'ont que des prévisions, le reste est `null` (257 137 lignes, soit la moitié du jeu). Le temps réel, lui, est entièrement au quart d'heure.
2. **Lignes futures dans le temps réel** : le jeu contient les prévisions jusqu'au lendemain. Sur ces lignes, toutes les valeurs réalisées sont `null`.
3. **Valeurs numériques typées en texte** : `gaz_cogen` et `ech_comm_allemagne_belgique` dans l'historique, `hydraulique_fil_eau_eclusee` dans le temps réel. Le contenu est numérique (par exemple `-3170`), mais le type déclaré est `text`, et le type diffère d'un jeu à l'autre pour un même champ.
4. **Schémas différents** : quatre champs n'existent que dans le temps réel (voir plus haut). La table silver doit les accepter vides pour l'historique.
5. **Fuseaux horaires** : `date_heure` est en UTC. Les champs `date` et `heure` sont en heure locale : à l'automne, la même heure locale apparaît deux fois.
6. **Description du portail en retard sur les données** : la description de l'historique annonce des données définitives jusqu'en décembre 2024 et consolidées depuis janvier 2021. Les données elles-mêmes sont définitives jusqu'au 31 décembre 2025. Ce document s'appuie sur les données.
7. **Lignes artificielles au passage à l'heure d'été** : le dernier dimanche de mars, l'heure locale passe de 01:59 à 03:00, et l'heure 02:00–02:45 n'existe pas. L'historique contient pourtant des lignes datées de 02:00 à 02:45 en heure locale (`heure`), dont `date_heure` est le même que celui des lignes de 03:00 à 03:45. Leurs valeurs réalisées sont recopiées de l'heure précédente : à 02:30 le 25 mars 2012, la consommation (47 196 MW) est celle de 01:30. Cela fait 4 quarts d'heure par an, soit 60 lignes en double sur `date_heure` de 2012 à 2026, comptées dans le total de l'API. La couche bronze les conserve, la couche silver doit garder les lignes de 03:xx et écarter celles de 02:xx.
8. **Heure manquante au passage à l'heure d'hiver** : le dernier dimanche d'octobre, l'heure locale 02:00–02:59 existe deux fois, d'abord en heure d'été (00:00–00:59 UTC) puis en heure d'hiver (01:00–01:59 UTC). La source n'en publie qu'une, la seconde : aucune ligne entre 00:00 et 00:59 UTC ces jours-là (vérifié dans l'API pour 2024 et 2025). Il manque donc 4 quarts d'heure par an, soit 2 demi-heures de valeurs réalisées, et la journée ne compte que 48 demi-heures au lieu de 50. La source semble indexée sur l'heure locale, une ligne par heure locale : elle invente l'heure qui n'existe pas en mars (particularité 7) et perd l'une des deux heures répétées en octobre. La couche gold signale ces journées comme incomplètes.

## Accès

L'API propose deux types d'appel :

| Endpoint | Usage | Limites observées |
| --- | --- | --- |
| `/catalog/datasets/{id}/records` | Requêtes paginées, filtres, agrégations | 100 lignes par page au plus, offset + limit ≤ 10 000 |
| `/catalog/datasets/{id}/exports/{format}` | Export complet ou filtré, en un seul appel | Formats : Parquet, CSV, JSON, JSONL, XLSX, etc. |

La limite de 10 000 lignes de `/records` ne permet pas de lire l'historique (514 272 lignes) : il faut passer par `/exports`.

Les en-têtes de réponse annoncent les quotas suivants : 10 000 000 d'appels par jour au total, et 50 000 appels par mois sur un même jeu de données.

Exemples :

```bash
API=https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets

# Les 4 derniers quarts d'heure du temps réel
curl -G "$API/eco2mix-national-tr/records" \
  --data-urlencode "order_by=date_heure desc" --data-urlencode "limit=4"

# Un mois d'historique au format Parquet
curl -G "$API/eco2mix-national-cons-def/exports/parquet" \
  --data-urlencode "where=date_heure >= '2026-08-01' and date_heure < '2026-09-01'" \
  -o eco2mix-2026-08.parquet
```

## Conditions de réutilisation

Les deux jeux sont publiés sous [Licence Ouverte v2.0 (Etalab)](https://www.etalab.gouv.fr/wp-content/uploads/2017/04/ETALAB-Licence-Ouverte-v2.0.pdf). Elle autorise la réutilisation, y compris commerciale, à condition de « mentionner la paternité de l'Information : sa source (au moins le nom du Concédant) et la date de dernière mise à jour de l'Information réutilisée ». Ici, le concédant est RTE.

Mention utilisée dans WattLake : « Source : RTE, données éCO2mix publiées sur ODRÉ », suivie de la date de mise à jour du jeu concerné.

## Sources

- Données éCO2mix nationales consolidées et définitives : https://odre.opendatasoft.com/explore/dataset/eco2mix-national-cons-def/
- Données éCO2mix nationales temps réel : https://odre.opendatasoft.com/explore/dataset/eco2mix-national-tr/
- Métadonnées et champs, via l'API : https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/eco2mix-national-cons-def
- Licence Ouverte v2.0 : https://www.etalab.gouv.fr/wp-content/uploads/2017/04/ETALAB-Licence-Ouverte-v2.0.pdf
