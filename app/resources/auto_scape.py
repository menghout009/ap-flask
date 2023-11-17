import pytz
from flask import Flask, request
from flask_restx import Namespace, Resource, fields
import requests
from lxml import html
import lxml.etree as etree
from cssselect import HTMLTranslator, SelectorError
import re
from lxml import html, etree
from lxml.cssselect import CSSSelector  # Import CSSSelector
from datetime import datetime

ns_link = Namespace('api/v1/auto_scrape', description='Webpage Data Extraction')
phnom_penh = pytz.timezone('Asia/Phnom_Penh')
current_datetime = datetime.now(phnom_penh)
extraction_data_model = ns_link.model('ExtractionData', {
    'link': fields.String(required=True, description='Link to identify the data in the response'),
    'method': fields.String(required=True, description='Extraction method (xpath or css)'),
    'extractions': fields.Raw(description='Extraction instructions')
})


# Function to validate a URL
def is_valid_url(url):
    try:
        response = requests.get(url)
        return response.status_code == 200
    except requests.RequestException:
        return False


@ns_link.route('/extract_data')
class ExtractResource(Resource):
    @ns_link.expect(extraction_data_model, validate=True)
    def post(self):
        extraction_data = request.json
        link = extraction_data['link']
        method = extraction_data['method'].lower()
        # extractions = extraction_data.get('extractions', {})  # Get the extraction instructions
        if not is_valid_url(link):
            return {
                'success': False,
                'message': 'Invalid or inaccessible URL.'
            }, 400

        if method not in ['xpath', 'css']:
            return {
                'success': False,
                'message': 'Invalid extraction method. Please choose "xpath" or "css".'
            }, 400
        try:
            response = requests.get(link)
            if response.status_code == 200:
                tree = html.fromstring(response.content)
                response_data = {}

                for key, selector in extraction_data['extractions'].items():
                    try:
                        if method == 'xpath':
                            try:
                                xpath_selector = etree.XPath(selector)
                                extracted_elements = xpath_selector(tree)
                                extracted_value = [
                                    element.strip() if isinstance(element, str) else etree.tostring(element).decode()
                                    for element in extracted_elements]
                                # extracted_value = tree.xpath(selector)
                            except SelectorError as e:
                                return {
                                    'success': False,
                                    'message': f'Invalid Xpath selector: {str(e)}'
                                }, 400
                        elif method == 'css':
                            if '::text' not in selector:
                                selector = selector.replace('::text', '')
                            try:
                                css_selector = CSSSelector(selector)
                                extracted_elements = css_selector(tree)
                                extracted_value = [element.text_content() for element in extracted_elements]
                                # extracted_value = [etree.tostring(element).decode() for element in extracted_elements]
                            except SelectorError as e:
                                return {
                                    'success': False,
                                    'message': f'Invalid CSS selector: {str(e)}'
                                }, 400

                        if not extracted_value:
                            raise ValueError("No matching elements found")

                        # extracted_value = [str(value) for value in extracted_value]
                        extracted_value = [str(value.strip()) if isinstance(value, str) else value for value in
                                           extracted_value]
                        # extracted_value = [str(html.tostring(value, encoding="unicode")) for value in extracted_value]

                        response_data[key] = extracted_value
                    except (etree.XPathSyntaxError, ValueError) as e:
                        return {
                            'success': False,
                            'message': f'Invalid {method} selector: {str(e)}'
                        }, 400
                return {
                    'date': current_datetime.isoformat(),
                    'success': True,
                    'message': 'get data successfully',
                    'payload': response_data
                }, 200
            else:
                return {
                    'success': False,
                    'message': 'Failed to fetch the webpage'
                }, 400
        except Exception as e:
            return {
                'success': False,
                'message': 'An error occurred while processing the request',
                'error': str(e)
            }, 404
