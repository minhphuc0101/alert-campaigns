import pandas as pd
import unittest
from unittest.mock import MagicMock, patch
import json

# Import the functions to test
from main import fetch_meta_ad_creatives

# Copy the original mock logic for test_coverage_logic
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

class TestCreativeParsing(unittest.TestCase):
    @patch('main.AdAccount')
    @patch('main.FacebookAdsApi')
    def test_fetch_meta_ad_creatives(self, mock_api, mock_ad_account_cls):
        # Setup the mock AdAccount instance
        mock_account_instance = MagicMock()
        mock_ad_account_cls.return_value = mock_account_instance
        
        # Define mock ads returned by account.get_ads
        mock_ads = []
        
        # 1. Normal creative, standard_enhancements OPT_IN
        mock_ads.append({
            'campaign_id': 'camp_1',
            'creative': {
                'degrees_of_freedom_spec': {
                    'creative_features_spec': {
                        'standard_enhancements': {
                            'enroll_status': 'OPT_IN'
                        }
                    }
                }
            }
        })
        
        # 2. Creative is None (Verify robustness)
        mock_ads.append({
            'campaign_id': 'camp_2',
            'creative': None
        })
        
        # 3. Creative exists but has NO degrees_of_freedom_spec
        mock_ads.append({
            'campaign_id': 'camp_3',
            'creative': {
                'id': 'cr_3'
            }
        })
        
        # 4. Standard enhancements with nested sub_enhancements
        mock_ads.append({
            'campaign_id': 'camp_4',
            'creative': {
                'degrees_of_freedom_spec': {
                    'creative_features_spec': {
                        'standard_enhancements': {
                            'enroll_status': 'OPT_IN',
                            'sub_enhancements': {
                                'image_touchups': {'enroll_status': 'OPT_IN'},
                                'text_optimizations': {'enroll_status': 'OPT_OUT'}
                            }
                        }
                    }
                }
            }
        })
        
        # 5. Granular features directly under creative_features_spec
        mock_ads.append({
            'campaign_id': 'camp_5',
            'creative': {
                'degrees_of_freedom_spec': {
                    'creative_features_spec': {
                        'image_touchups': {'enroll_status': 'OPT_IN'},
                        'text_optimizations': {'enroll_status': 'OPT_IN'},
                        'inline_comment': {'enroll_status': 'OPT_OUT'}
                    }
                }
            }
        })
        
        # 6. JSON string format of degrees_of_freedom_spec
        mock_ads.append({
            'campaign_id': 'camp_6',
            'creative': {
                'degrees_of_freedom_spec': json.dumps({
                    'creative_features_spec': {
                        'standard_enhancements': {
                            'enroll_status': 'OPT_IN'
                        }
                    }
                })
            }
        })
        
        # 7. dof on the ad level itself
        mock_ads.append({
            'campaign_id': 'camp_7',
            'degrees_of_freedom_spec': {
                'creative_features_spec': {
                    'standard_enhancements': {
                        'enroll_status': 'OPT_IN'
                    }
                }
            }
        })
        
        # Set the mock get_ads return value
        mock_account_instance.get_ads.return_value = mock_ads
        
        # Run parsing
        result, err = fetch_meta_ad_creatives('mock_token', ['12345'])
        
        # Check result
        self.assertIsNone(err)
        self.assertIn('act_12345', result)
        
        account_results = result['act_12345']
        
        # camp_1 should have 'Standard Enhancements'
        self.assertIn('camp_1', account_results)
        self.assertEqual(account_results['camp_1'], {'Standard Enhancements'})
        
        # camp_2 should not be in results since creative is None
        self.assertNotIn('camp_2', account_results)
        
        # camp_3 should not be in results
        self.assertNotIn('camp_3', account_results)
        
        # camp_4 should have 'Standard Enhancements (Image Touchups)'
        self.assertIn('camp_4', account_results)
        self.assertEqual(account_results['camp_4'], {'Standard Enhancements (Image Touchups)'})
        
        # camp_5 should have 'Image Touchups' and 'Text Optimizations'
        self.assertIn('camp_5', account_results)
        self.assertEqual(account_results['camp_5'], {'Image Touchups', 'Text Optimizations'})
        
        # camp_6 should have 'Standard Enhancements' (parsed from string)
        self.assertIn('camp_6', account_results)
        self.assertEqual(account_results['camp_6'], {'Standard Enhancements'})
        
        # camp_7 should have 'Standard Enhancements' (parsed from ad level)
        self.assertIn('camp_7', account_results)
        self.assertEqual(account_results['camp_7'], {'Standard Enhancements'})
        
        print("Creative Parsing Verification Passed!")

if __name__ == '__main__':
    unittest.main()
