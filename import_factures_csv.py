import csv
import requests
from dotenv import load_dotenv
import os

load_dotenv()

SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

def nettoyer_montant(valeur):
    """
    Convertit "46.85" ou "46,85" en 46.85
    """
    try:
        if valeur is None or valeur == '':
            return None
        return float(str(valeur).replace(',', '.').strip())
    except:
        return None

def lire_csv_factures(chemin):
    """
    Lit le CSV et extrait les données électricité et gaz
    """
    factures_elec = []
    factures_gaz = []
    
    try:
        with open(chemin, 'r', encoding='utf-8-sig') as f:
            lecteur = csv.DictReader(f, delimiter=';')
            
            for ligne in lecteur:
                mois = ligne['Mois']  # Déjà au format 2023-04-01
                
                # ÉLECTRICITÉ
                elec_conso = nettoyer_montant(ligne.get('Electricité'))
                elec_abo = nettoyer_montant(ligne.get('Electricité_abonnement'))
                
                if elec_conso is not None and elec_abo is not None:
                    factures_elec.append({
                        'mois': mois,
                        'montant_consommation': elec_conso,
                        'montant_abonnement': elec_abo,
                        'consommation_kwh': None  # Pas dans votre CSV
                    })
                
                # GAZ
                gaz_conso = nettoyer_montant(ligne.get('Gaz'))
                gaz_abo = nettoyer_montant(ligne.get('Gaz_abonnement'))
                
                if gaz_conso is not None and gaz_abo is not None:
                    factures_gaz.append({
                        'mois': mois,
                        'montant_consommation': gaz_conso,
                        'montant_abonnement': gaz_abo,
                        'consommation_kwh': None  # Pas dans votre CSV
                    })
        
        print(f"✅ CSV lu : {len(factures_elec)} lignes électricité, {len(factures_gaz)} lignes gaz\n")
        return factures_elec, factures_gaz
        
    except FileNotFoundError:
        print(f"❌ Fichier non trouvé : {chemin}")
        return None, None
    except Exception as e:
        print(f"❌ Erreur lecture CSV : {e}")
        import traceback
        traceback.print_exc()
        return None, None

def importer_dans_supabase(factures, table_name, type_energie):
    """
    Importe les factures dans Supabase
    """
    if not factures:
        print(f"⚠️  Aucune donnée à importer dans {table_name}")
        return
    
    url = f"{SUPABASE_URL}/rest/v1/{table_name}"
    
    headers = {
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Content-Type': 'application/json',
        'Prefer': 'resolution=merge-duplicates'
    }
    
    succes = 0
    erreurs = 0
    doublons = 0
    
    for facture in factures:
        try:
            # Ne pas envoyer consommation_kwh si elle est None
            data = {
                'mois': facture['mois'],
                'montant_consommation': facture['montant_consommation'],
                'montant_abonnement': facture['montant_abonnement']
            }
            
            if facture.get('consommation_kwh') is not None:
                data['consommation_kwh'] = facture['consommation_kwh']
            
            response = requests.post(url, json=data, headers=headers, timeout=10)
            
            mois_affiche = facture['mois'][:7]  # 2023-04
            total = facture['montant_consommation'] + facture['montant_abonnement']
            
            if response.status_code in [200, 201]:
                succes += 1
                print(f"✅ {mois_affiche} : {total:>7.2f}€ (conso: {facture['montant_consommation']:>6.2f}€ + abo: {facture['montant_abonnement']:>5.2f}€)")
            elif response.status_code == 409:
                doublons += 1
                print(f"ℹ️  {mois_affiche} : déjà existant")
            else:
                erreurs += 1
                print(f"⚠️  {mois_affiche} : erreur {response.status_code}")
                print(f"    Réponse : {response.text[:100]}")
                
        except Exception as e:
            erreurs += 1
            print(f"❌ Erreur {facture['mois'][:7]} : {e}")
    
    print(f"\n📊 Résumé {type_energie} :")
    print(f"   ✅ Nouvelles factures : {succes}")
    print(f"   ℹ️  Doublons ignorés   : {doublons}")
    print(f"   ❌ Erreurs            : {erreurs}\n")

def importer_factures(chemin_csv):
    """
    Fonction principale d'import
    """
    print("\n" + "="*70)
    print("📊 IMPORT DES FACTURES DEPUIS CSV")
    print("="*70 + "\n")
    
    # Lire le CSV
    factures_elec, factures_gaz = lire_csv_factures(chemin_csv)
    
    if factures_elec is None:
        return
    
    # Afficher un résumé
    print("="*70)
    if factures_elec:
        total_elec = sum(f['montant_consommation'] + f['montant_abonnement'] for f in factures_elec)
        total_conso_elec = sum(f['montant_consommation'] for f in factures_elec)
        total_abo_elec = sum(f['montant_abonnement'] for f in factures_elec)
        
        print(f"\n⚡ ÉLECTRICITÉ : {len(factures_elec)} factures")
        print(f"   Période : {factures_elec[0]['mois'][:7]} → {factures_elec[-1]['mois'][:7]}")
        print(f"   Total consommation : {total_conso_elec:>8.2f}€")
        print(f"   Total abonnement   : {total_abo_elec:>8.2f}€")
        print(f"   TOTAL              : {total_elec:>8.2f}€")
    
    if factures_gaz:
        total_gaz = sum(f['montant_consommation'] + f['montant_abonnement'] for f in factures_gaz)
        total_conso_gaz = sum(f['montant_consommation'] for f in factures_gaz)
        total_abo_gaz = sum(f['montant_abonnement'] for f in factures_gaz)
        
        print(f"\n🔥 GAZ : {len(factures_gaz)} factures")
        print(f"   Période : {factures_gaz[0]['mois'][:7]} → {factures_gaz[-1]['mois'][:7]}")
        print(f"   Total consommation : {total_conso_gaz:>8.2f}€")
        print(f"   Total abonnement   : {total_abo_gaz:>8.2f}€")
        print(f"   TOTAL              : {total_gaz:>8.2f}€")
    
    print("\n" + "="*70)
    
    # Demander confirmation
    reponse = input("\nImporter ces données dans Supabase ? (o/n) : ").strip().lower()
    
    if reponse not in ['o', 'oui', 'y', 'yes']:
        print("\n❌ Import annulé\n")
        return
    
    # Importer
    print("\n" + "="*70)
    print("⚡ IMPORT ÉLECTRICITÉ")
    print("="*70 + "\n")
    importer_dans_supabase(factures_elec, 'factures_electricite', 'Électricité')
    
    print("="*70)
    print("🔥 IMPORT GAZ")
    print("="*70 + "\n")
    importer_dans_supabase(factures_gaz, 'factures_gaz', 'Gaz')
    
    print("="*70)
    print("✅ IMPORT TERMINÉ")
    print("="*70 + "\n")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        chemin = sys.argv[1]
    else:
        print("\n📁 Glissez-déposez votre fichier CSV ici, puis appuyez sur Entrée :")
        chemin = input().strip().strip('"')
    
    importer_factures(chemin)
