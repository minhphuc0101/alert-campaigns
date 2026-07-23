import os
import json
from dotenv import load_dotenv
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.user import User

load_dotenv()

access_token = os.environ.get('FB_ACCESS_TOKEN')
if not access_token:
    print("Error: FB_ACCESS_TOKEN is not set in environment or .env file.")
    exit(1)

FacebookAdsApi.init(access_token=access_token)
me = User(fbid='me')

print("Fetching Ad Accounts...")
try:
    my_accounts = me.get_ad_accounts(fields=['id', 'name'], params={'limit': 500})
except Exception as e:
    print(f"Error fetching ad accounts: {e}")
    exit(1)

target_accounts = []
for acc in my_accounts:
    name = acc.get('name', '')
    if name.strip().upper().startswith('DIGIMIND_'):
        target_accounts.append(acc)

print(f"Found {len(target_accounts)} DIGIMIND accounts:")
for acc in target_accounts:
    print(f" - {acc.get('name')} ({acc.get('id')})")

debug_log = {}

for acc in target_accounts:
    acc_id = acc['id']
    acc_name = acc.get('name')
    print(f"\nProcessing Account: {acc_name} ({acc_id})...")
    
    account = AdAccount(acc_id)
    try:
        # Request creative with id and degrees_of_freedom_spec
        ads = account.get_ads(
            fields=['campaign_id', 'creative{id,degrees_of_freedom_spec}', 'name', 'degrees_of_freedom_spec'],
            params={'filtering': '[{"field":"effective_status","operator":"IN","value":["ACTIVE"]}]'}
        )
    except Exception as e:
        print(f"  Error fetching ads: {e}")
        continue
        
    print(f"  Found {len(ads)} active ads.")
    account_ads_data = []
    
    for ad in ads:
        ad_id = ad.get('id')
        ad_name = ad.get('name')
        camp_id = ad.get('campaign_id')
        creative = ad.get('creative')
        
        ad_data = {
            'ad_id': ad_id,
            'ad_name': ad_name,
            'campaign_id': camp_id,
            'creative_degrees_of_freedom_spec': None,
            'ad_degrees_of_freedom_spec': None
        }
        
        if creative:
            try:
                dof = creative.get('degrees_of_freedom_spec')
                ad_data['creative_degrees_of_freedom_spec'] = dof
                if dof:
                    print(f"  [FOUND ON CREATIVE] Ad: '{ad_name}' (Camp: {camp_id}) has dof in creative!")
            except Exception as ex:
                print(f"  Error getting dof from creative: {ex}")
                
        try:
            dof_ad = ad.get('degrees_of_freedom_spec')
            ad_data['ad_degrees_of_freedom_spec'] = dof_ad
            if dof_ad:
                 print(f"  [FOUND ON AD] Ad: '{ad_name}' (Camp: {camp_id}) has dof on ad level!")
        except Exception as ex:
             # This is expected if the field is not valid on Ad object
             pass
             
        account_ads_data.append(ad_data)
        
    debug_log[acc_name] = account_ads_data

with open('debug_creatives_output.json', 'w', encoding='utf-8') as f:
    json.dump(debug_log, f, indent=2, ensure_ascii=False)

print("\nFinished! Results saved to debug_creatives_output.json")
