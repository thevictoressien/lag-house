import unittest
from unittest.mock import patch
from dags.scripts.house_scrapper import scrape_and_upload, extract_listing_data

class TestHouseScrapper(unittest.TestCase):

    @patch('src.house_scrapper.upload_to_gcs')
    def test_scrape_and_upload(self, mock_upload):
        scrape_and_upload('rent', bucket_name='test-bucket', file_name='test.csv', base_url='http://test.com', city='testcity')
        mock_upload.assert_called_once()

    def test_extract_listing_data(self):
        mock_listings = [
            type('obj', (object,), {
                'content': '<html><body><address>123 Test St</address><td>Market Status: Available</td></body></html>'
            })
        ]
        result = extract_listing_data(mock_listings)
        self.assertEqual(result[0]['location'], '123 Test St')
        self.assertEqual(result[0]['status'], 'Available')

if __name__ == '__main__':
    unittest.main()
