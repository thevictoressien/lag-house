import pytest
from unittest.mock import Mock, patch
from dags.scripts.house_scrapper import create_session, fetch_page, extract_listing_data, upload_to_gcs, process_chunk, house_scrapper

def test_create_session():
    session = create_session()
    assert session.adapters['http://'].max_retries.total == 15
    assert session.adapters['https://'].max_retries.total == 15

@patch('dags.scripts.house_scrapper.requests.Session')
def test_fetch_page(mock_session):
    mock_response = Mock()
    mock_response.raise_for_status.return_value = None
    mock_session.return_value.get.return_value = mock_response
    
    headers = {'User-Agent': 'Test'}
    response = fetch_page(mock_session(), 'http://test.com', headers)
    
    assert response == mock_response
    mock_session.return_value.get.assert_called_once_with('http://test.com', headers=headers)

def test_extract_listing_data():
    mock_listing = Mock()
    mock_listing.content = '<html><body><address>Test Address</address><span class="price" itemprop="price" content="1000">$1000</span></body></html>'
    
    data = extract_listing_data([mock_listing])
    
    assert len(data) == 1
    assert data[0]['location'] == 'Test Address'
    assert data[0]['price'] == '1000'

@patch('dags.scripts.house_scrapper.GCSManager')
def test_upload_to_gcs(mock_gcs_manager):
    upload_to_gcs('test-bucket', 'test-file.csv', 'test,data')
    
    mock_gcs_manager.return_value.upload_file_from_string.assert_called_once_with(
        bucket_name='test-bucket',
        buffer='test,data',
        destination_blob_name='test-file.csv',
        content_type='text/csv'
    )

@patch('dags.scripts.house_scrapper.fetch_page')
@patch('dags.scripts.house_scrapper.extract_listing_data')
def test_process_chunk(mock_extract_listing_data, mock_fetch_page):
    mock_session = Mock()
    mock_response = Mock()
    mock_response.content = '<html></html>'
    mock_fetch_page.return_value = mock_response
    mock_extract_listing_data.return_value = [{'location': 'Test', 'price': '1000'}]
    
    result = process_chunk(mock_session, {}, 'http://test.com', 'sale', 'testcity', 1, 2)
    
    assert 'location,status,bedrooms' in result
    print(result)
    assert 'Test,,,,,,,,,,,1000,' in result

@patch('dags.scripts.house_scrapper.process_chunk')
@patch('dags.scripts.house_scrapper.upload_to_gcs')
def test_house_scrapper(mock_upload_to_gcs, mock_process_chunk):
    mock_process_chunk.return_value = 'test,data'
    
    house_scrapper('test-bucket', 'test-file.csv', 'http://test.com', 'sale', 'testcity', 1, 2)
    
    mock_upload_to_gcs.assert_called()
    mock_process_chunk.assert_called()
