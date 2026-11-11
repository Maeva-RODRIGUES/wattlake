# streaming

Producteur temps réel : il interroge le jeu éCO2mix temps réel (`eco2mix-national-tr`) et publie ses mesures dans le topic Kafka `eco2mix-realtime`. Les choix sont expliqués dans l'[ADR 009](../docs/adr/009-producteur-temps-reel.md).

## Fonctionnement

Le producteur tourne en continu dans un `Deployment` à un réplica ([infra/streaming/](../infra/streaming/)). Toutes les 5 minutes :

1. il lit la date de rafraîchissement du jeu (`data_processed`) ; si elle n'a pas changé, il attend le cycle suivant ;
2. sinon, il exporte les lignes mesurées depuis son curseur moins 3 heures, pour republier les valeurs que RTE a pu réviser ;
3. il les publie, de la plus ancienne à la plus récente, et attend que Kafka les ait toutes acceptées ;
4. il avance son curseur au dernier quart d'heure publié.

Le curseur n'est stocké nulle part ailleurs que dans Kafka : au démarrage, c'est la clé du dernier message du topic. Le topic vide, le producteur publie tout le jeu, depuis le 1ᵉʳ septembre.

Si l'API ne répond pas, après 5 tentatives espacées, le cycle est sauté et le suivant rattrape tout. Une erreur de Kafka arrête le processus : Kubernetes le redémarre, et il reprend depuis le topic.

| Module | Rôle |
| --- | --- |
| `odre.py` | Client de l'API ODRÉ, avec nouvelles tentatives sur les erreurs passagères |
| `messages.py` | Format des messages (clé, enveloppe JSON) |
| `kafka.py` | Publication avec accusés de réception, lecture du dernier message du topic |
| `producer.py` | Boucle, configuration, arrêt propre sur SIGTERM |
| `health.py` | Sonde de vie : échoue si le fichier de heartbeat a plus de 15 minutes |

## Format des messages

- **Topic** : `eco2mix-realtime`, une partition, compacté ([infra/kafka/topics.yaml](../infra/kafka/topics.yaml)).
- **Clé** : le quart d'heure, `date_heure` tel que publié par l'API, en UTC : `2026-11-11T16:00:00+00:00`.
- **Valeur** : du JSON en UTF-8.

```json
{
  "dataset": "eco2mix-national-tr",
  "source_updated_at": "2026-11-11T17:00:14+00:00",
  "record": {"date_heure": "2026-11-11T16:00:00+00:00", "consommation": 47025, "nucleaire": 42257, "...": "..."}
}
```

`record` est la ligne de l'API sans aucune modification : noms et types des champs compris, y compris les nombres typés en texte (voir [docs/sources.md](../docs/sources.md)). `source_updated_at` est la date de rafraîchissement du jeu au moment de la lecture.

Un même quart d'heure peut arriver plusieurs fois : le consommateur garde la version au `source_updated_at` le plus récent. Grâce à la compaction, Kafka finit par ne garder que la dernière version de chaque clé.

## Configuration

Variables d'environnement, avec leur valeur par défaut :

| Variable | Défaut | Rôle |
| --- | --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | `wattlake-kafka-bootstrap:9092` | Adresse de Kafka |
| `KAFKA_TOPIC` | `eco2mix-realtime` | Topic de destination |
| `POLL_INTERVAL_SECONDS` | `300` | Intervalle entre deux cycles |
| `OVERLAP_HOURS` | `3` | Fenêtre republiée à chaque rafraîchissement |
| `HEARTBEAT_FILE` | `/tmp/heartbeat` | Fichier touché à chaque cycle, lu par la sonde de vie |

## Commandes

```bash
make streaming-test         # tests unitaires, sans réseau (API et Kafka simulés)
make streaming-lock         # régénère requirements.txt et requirements-test.txt
make producer-image         # construit l'image wattlake-producer:local
make producer-image-import  # charge cette image dans les nœuds k3d
```

Les dépendances sont déclarées dans `pyproject.toml`, puis figées avec leurs empreintes (hashes) par `make streaming-lock`, qui ignore les versions de moins de 14 jours. L'image installe `requirements.txt` avec `--require-hashes`.

Suivre le producteur et lire le topic :

```bash
kubectl --context k3d-wattlake -n wattlake logs deploy/producer -f
kubectl --context k3d-wattlake -n wattlake exec wattlake-dual-role-0 -- \
  /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic eco2mix-realtime --from-beginning --max-messages 3 --formatter-property print.key=true
```
