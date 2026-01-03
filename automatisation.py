import schedule
import time
from meteo_collector import collecter_meteo_journaliere

# Planifier la collecte tous les jours à 7h00 du matin
schedule.every().day.at("07:00").do(collecter_meteo_journaliere)

print("🤖 Automatisation démarrée")
print("⏰ Collecte programmée tous les jours à 7h00")
print("👉 Appuyez sur Ctrl+C pour arrêter")

# Boucle infinie qui vérifie les tâches planifiées
while True:
    schedule.run_pending()
    time.sleep(60)  # Vérifier toutes les minutes