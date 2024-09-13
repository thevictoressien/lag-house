import unittest
from unittest.mock import patch, MagicMock
from dags.scripts.house_scrapper import scrape_and_upload, extract_listing_data

class TestHouseScrapper(unittest.TestCase):

    @patch('dags.scripts.house_scrapper.upload_to_gcs')
    @patch('dags.scripts.house_scrapper.Variable')
    def test_scrape_and_upload(self, mock_variable, mock_upload):
        mock_variable.get.return_value = 'test_value'
        scrape_and_upload(bucket_name='test-bucket', file_name='test.csv', base_url='http://test.com', category='rent', city='testcity')
        mock_upload.assert_called_once()

    def test_extract_listing_data(self):
        mock_listings = [
            MagicMock(content='<html><body><address>123 Test St</address><td>Market Status: Available</td></body></html>')
        ]
        result = extract_listing_data(mock_listings)
        self.assertEqual(result[0]['location'], '123 Test St')
        self.assertEqual(result[0]['status'], 'Available')

if __name__ == '__main__':
    unittest.main()
