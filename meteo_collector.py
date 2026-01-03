import requests
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import time

# Chargement des variables d'environnement
load_dotenv()

# Configuration
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')
LATITUDE = float(os.getenv('LATITUDE'))
LONGITUDE = float(os.getenv('LONGITUDE'))
VILLE = os.getenv('VILLE')

def recuperer_donnees_meteo_historique(date_debut, date_fin):
    """
    Récupère les données météo historiques via Open-Meteo Archive API
    """
    url = "https://archive-api.open-meteo.com/v1/archive"
    
    params = {
        'latitude': LATITUDE,
        'longitude': LONGITUDE,
        'daily': [
            'temperature_2m_max',
            'temperature_2m_min',
            'temperature_2m_mean',
            'precipitation_sum',
            'wind_speed_10m_max',
            'relative_humidity_2m_mean',
            'pressure_msl_mean',
            'cloud_cover_mean'
        ],
        'start_date': date_debut,
        'end_date': date_fin,
        'timezone': 'Europe/Paris'
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ Erreur lors de la récupération des données météo : {e}")
        return None

def recuperer_donnees_meteo_recentes(date_debut, date_fin):
    """
    Récupère les données météo récentes via Open-Meteo Forecast API
    """
    url = "https://api.open-meteo.com/v1/forecast"
    
    params = {
        'latitude': LATITUDE,
        'longitude': LONGITUDE,
        'daily': [
            'temperature_2m_max',
            'temperature_2m_min',
            'temperature_2m_mean',
            'precipitation_sum',
            'wind_speed_10m_max',
            'relative_humidity_2m_mean',
            'pressure_msl_mean',
            'cloud_cover_mean'
        ],
        'start_date': date_debut,
        'end_date': date_fin,
        'timezone': 'Europe/Paris',
        'past_days': 92
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ Erreur lors de la récupération des données météo : {e}")
        return None

def sauvegarder_dans_supabase(donnees_meteo):
    """
    Sauvegarde les données dans Supabase via l'API REST
    """
    if not donnees_meteo or 'daily' not in donnees_meteo:
        print("❌ Pas de données météo à sauvegarder")
        return False
    
    daily = donnees_meteo['daily']
    dates = daily['time']
    
    # URL de l'API Supabase pour la table meteo_data
    url = f"{SUPABASE_URL}/rest/v1/meteo_data"
    
    # Headers pour l'authentification
    headers = {
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Content-Type': 'application/json',
        'Prefer': 'resolution=merge-duplicates'
    }
    
    succes = 0
    erreurs = 0
    
    for i in range(len(dates)):
        donnee = {
            'date_mesure': dates[i],
            'temperature_min': daily['temperature_2m_min'][i],
            'temperature_max': daily['temperature_2m_max'][i],
            'temperature_moyenne': daily['temperature_2m_mean'][i],
            'humidite': daily['relative_humidity_2m_mean'][i],
            'precipitation': daily['precipitation_sum'][i],
            'vitesse_vent': daily['wind_speed_10m_max'][i],
            'pression': daily['pressure_msl_mean'][i],
            'couverture_nuageuse': daily['cloud_cover_mean'][i],
            'ville': VILLE
        }
        
        try:
            response = requests.post(url, json=donnee, headers=headers, timeout=10)
            
            if response.status_code in [200, 201]:
                succes += 1
                print(f"✅ {dates[i]} : données sauvegardées")
            elif response.status_code == 409:
                # Doublon - on met à jour
                print(f"ℹ️  {dates[i]} : donnée déjà existante (ignorée)")
                succes += 1
            else:
                print(f"⚠️  {dates[i]} : erreur {response.status_code}")
                erreurs += 1
                
        except requests.exceptions.RequestException as e:
            print(f"❌ {dates[i]} : erreur réseau - {e}")
            erreurs += 1
    
    print(f"\n📊 Résumé partiel : {succes} succès, {erreurs} erreurs")
    return erreurs == 0

def collecter_meteo_journaliere():
    """
    Fonction principale de collecte quotidienne
    """
    print(f"\n🌤️  Début de la collecte météo - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Récupérer les données de la veille
    hier = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    print(f"📅 Récupération des données pour le {hier}")
    
    # Utiliser l'API Archive pour les données d'hier
    donnees = recuperer_donnees_meteo_historique(hier, hier)
    
    if donnees:
        if sauvegarder_dans_supabase(donnees):
            print("✅ Collecte terminée avec succès")
        else:
            print("⚠️  Collecte terminée avec des erreurs")
    else:
        print("❌ Échec de la collecte")

def collecter_historique(nb_jours=30):
    """
    Récupère l'historique sur les N derniers jours
    Utile pour la première exécution
    """
    print(f"\n📊 Collecte de l'historique sur {nb_jours} jours")
    
    date_fin = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    date_debut = (datetime.now() - timedelta(days=nb_jours)).strftime('%Y-%m-%d')
    
    print(f"📅 Période : {date_debut} à {date_fin}")
    
    donnees = recuperer_donnees_meteo_recentes(date_debut, date_fin)
    
    if donnees:
        if sauvegarder_dans_supabase(donnees):
            print("✅ Historique collecté avec succès")
        else:
            print("⚠️  Historique collecté avec des erreurs")
    else:
        print("❌ Échec de la collecte de l'historique")

def collecter_depuis_date(date_debut_str):
    """
    Récupère l'historique depuis une date précise
    Utilise uniquement l'API Archive pour toutes les données passées
    date_debut_str : format 'YYYY-MM-DD', ex: '2023-04-01'
    """
    print(f"\n📊 Collecte de l'historique depuis {date_debut_str}")
    
    date_debut = datetime.strptime(date_debut_str, '%Y-%m-%d')
    date_fin_totale = datetime.now() - timedelta(days=1)
    
    print(f"📅 Période totale : {date_debut_str} à {date_fin_totale.strftime('%Y-%m-%d')}")
    
    # Découper en périodes de 365 jours maximum
    periode_jours = 365
    date_courante = date_debut
    
    while date_courante <= date_fin_totale:
        date_fin_periode = min(date_courante + timedelta(days=periode_jours - 1), date_fin_totale)
        
        date_debut_str_periode = date_courante.strftime('%Y-%m-%d')
        date_fin_str_periode = date_fin_periode.strftime('%Y-%m-%d')
        
        print(f"\n🔄 Traitement de la période : {date_debut_str_periode} à {date_fin_str_periode}")
        
        donnees = recuperer_donnees_meteo_historique(date_debut_str_periode, date_fin_str_periode)
        
        if donnees:
            sauvegarder_dans_supabase(donnees)
        else:
            print(f"⚠️  Échec pour cette période")
        
        date_courante = date_fin_periode + timedelta(days=1)
        time.sleep(1)  # Pause pour ne pas surcharger l'API
    
    print(f"\n✅ Collecte de l'historique terminée !")

def test_connexion():
    """
    Teste la connexion à Supabase
    """
    print("🔧 Test de connexion à Supabase...")
    
    url = f"{SUPABASE_URL}/rest/v1/meteo_data?limit=1"
    headers = {
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            print("✅ Connexion à Supabase réussie !")
            return True
        else:
            print(f"❌ Erreur de connexion : {response.status_code}")
            print(f"   Message : {response.text}")
            return False
    except Exception as e:
        print(f"❌ Erreur de connexion : {e}")
        return False

if __name__ == "__main__":
    # Test de connexion d'abord
    if not test_connexion():
        print("\n⚠️  Vérifiez votre fichier .env et vos clés Supabase")
        exit(1)
    
    # Récupérer l'historique depuis votre emménagement (1er avril 2023)
    collecter_depuis_date('2025-10-01')
    
    # Pour la collecte quotidienne, utilisez plutôt :
    # collecter_meteo_journaliere()