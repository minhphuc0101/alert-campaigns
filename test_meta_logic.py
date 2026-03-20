
import pandas as pd
import unittest
from unittest.mock import MagicMock

# Import the functions to test (or copy them if importing is hard in this environment)
# For simplicity in this scratch test, I'll copy the logic we want to verify

def analyze_data_mock(df, meta_rules, ad_account_col, campaign_id_col, campaign_name_col):
    results = []
    for _, row in df.iterrows():
        campaign = row[campaign_name_col]
        acc_id = str(row[ad_account_col])
        full_acc_id = acc_id if acc_id.startswith('act_') else f"act_{acc_id}"
        camp_id = str(row[campaign_id_col])
        
        is_covered = False
        if full_acc_id in meta_rules:
            acc_data = meta_rules[full_acc_id]
            protected_ids = acc_data.get('protected_ids', set())
            has_global_pause = acc_data.get('has_global_pause', False)
            
            is_covered = (camp_id and camp_id in protected_ids) or has_global_pause
            
        results.append({
            'campaign': campaign,
            'is_covered': is_covered
        })
    return results

class TestMetaAuditLogic(unittest.TestCase):
    def test_coverage_logic(self):
        # Setup mock rules
        meta_rules = {
            'act_123': {
                'protected_ids': {'101', '102'},
                'has_global_pause': False
            },
            'act_456': {
                'protected_ids': set(),
                'has_global_pause': True
            },
            'act_789': {
                'protected_ids': set(),
                'has_global_pause': False
            }
        }
        
        # Setup mock spreadsheet data
        df = pd.DataFrame([
            {'campaign name': 'C1', 'acc_id': '123', 'camp_id': '101'}, # Covered by ID
            {'campaign name': 'C2', 'acc_id': '123', 'camp_id': '999'}, # NOT covered
            {'campaign name': 'C3', 'acc_id': '456', 'camp_id': '888'}, # Covered by GLOBAL
            {'campaign name': 'C4', 'acc_id': '789', 'camp_id': '777'}, # NOT covered
        ])
        
        results = analyze_data_mock(df, meta_rules, 'acc_id', 'camp_id', 'campaign name')
        
        self.assertTrue(results[0]['is_covered'], "C1 should be covered by ID")
        self.assertFalse(results[1]['is_covered'], "C2 should not be covered")
        self.assertTrue(results[2]['is_covered'], "C3 should be covered by GLOBAL")
        self.assertFalse(results[3]['is_covered'], "C4 should not be covered")
        
        print("Meta Audit Logic Verification Passed!")

if __name__ == '__main__':
    unittest.main()
