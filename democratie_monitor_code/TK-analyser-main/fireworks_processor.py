import requests
import json
import logging
from typing import Dict, Optional
import signal
from contextlib import contextmanager

# Set up logging
logger = logging.getLogger(__name__)

class FireworksProcessor:
    def __init__(self, api_key: str):
        """Initialize the Fireworks processor with API credentials."""
        self.api_key = api_key
        self.base_url = "https://api.fireworks.ai/inference/v1/completions"
        self.headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}"
        }

    def process_text(self, text: str, prompt: str,
                    source_file: Optional[str] = None,
                    original_text: Optional[str] = None,
                    max_tokens: int = 2048,
                    api_timeout: int = 30,
                    parse_timeout: int = 5) -> Dict:
        """
        Process text using the Fireworks API.

        Args:
            text: Text to analyze
            prompt: Analysis prompt
            source_file: Source file location
            original_text: Original speech text
            max_tokens: Maximum tokens in response
            api_timeout: API request timeout in seconds
            parse_timeout: JSON parsing timeout in seconds

        Returns:
            Dict containing the analysis results
        """
        payload = {
            "model": "accounts/fireworks/models/llama-v3p3-70b-instruct",
            "max_tokens": max_tokens,
            "top_p": 1,
            "top_k": 40,
            "presence_penalty": 0,
            "frequency_penalty": 0,
            "temperature": 0.1,
            "prompt": f"{prompt}\n\nText to analyze:\n{text}"
        }

        logger.info(f"Processing text of length {len(text)}")
        try:
            with requests.Session() as session:
                session.headers.update(self.headers)
                response = session.post(
                    self.base_url,
                    json=payload,
                    timeout=api_timeout
                )
                logger.info(f"Received response (status: {response.status_code})")

                response.raise_for_status()
                result = response.json()

                # Parse the API response
                analysis_result = self._parse_api_response(result, parse_timeout)

                # Check if delegitimation was found
                has_delegitimation = (
                    isinstance(analysis_result, dict) and
                    (len(analysis_result.get('gevonden_delegitimatie', [])) > 0 or
                     len(analysis_result.get('gevallen_delegitimatie', [])) > 0)
                )

                # Add source information if delegitimation was found
                if has_delegitimation:
                    analysis_result['_meta'] = {
                        'source_file': source_file,
                        'original_text': original_text
                    }

                return analysis_result

        except requests.exceptions.Timeout:
            logger.error(f"API request timed out after {api_timeout} seconds")
            return self._get_error_response()
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            return self._get_error_response()
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return self._get_error_response()

    def _parse_api_response(self, result: Dict, timeout: int) -> Dict:
        """Parse the API response and extract the analysis results."""
        if 'choices' not in result or not result['choices']:
            logger.error("No completion in API response")
            return self._get_error_response()

        completion = result['choices'][0].get('text', '').strip()
        logger.debug(f"Raw completion text length: {len(completion)}")
        logger.debug(f"Raw completion text:\n{completion}")

        try:
            with self._timeout(timeout):
                # Pre-process the text
                clean_text = completion.replace('```json', '').replace('```', '')
                clean_text = clean_text.strip()

                # Try direct JSON parsing first
                try:
                    parsed_json = json.loads(clean_text)
                    logger.info(f"Successfully parsed complete JSON object:\n{json.dumps(parsed_json, indent=2)}")
                    return parsed_json
                except json.JSONDecodeError:
                    # If direct parsing fails, try extracting the first valid JSON
                    logger.debug("Direct JSON parsing failed, trying extraction")
                    return self._extract_json(clean_text)

        except TimeoutError:
            logger.error("JSON parsing timed out")
            return self._get_error_response()
        except Exception as e:
            logger.error(f"Error parsing response: {str(e)}")
            return self._get_error_response()

    def _extract_json(self, text: str) -> Dict:
        """Extract and parse the first complete JSON object from text."""
        logger.debug("Starting JSON extraction")

        # Find the first opening brace
        start_idx = text.find('{')
        if start_idx == -1:
            logger.error("No JSON object found in text")
            return self._get_error_response()

        # Parse until we find the matching closing brace
        brace_count = 0
        in_string = False
        escape_next = False
        result = []

        for i, char in enumerate(text[start_idx:], start=start_idx):
            result.append(char)

            if escape_next:
                escape_next = False
                continue

            if char == '\\':
                escape_next = True
                continue

            if char == '"' and not escape_next:
                in_string = not in_string
                continue

            if not in_string:
                if char == '{':
                    brace_count += 1
                elif char == '}':
                    brace_count -= 1
                    if brace_count == 0:
                        # We found a complete JSON object
                        json_str = ''.join(result)
                        try:
                            return json.loads(json_str)
                        except json.JSONDecodeError as e:
                            logger.error(f"Failed to parse extracted JSON: {e}")
                            logger.debug(f"Problematic JSON string: {json_str}")
                            continue

        logger.error("No valid JSON object found")
        return self._get_error_response()

    @staticmethod
    @contextmanager
    def _timeout(seconds: int):
        """Context manager for timeouts."""
        def handler(signum, frame):
            raise TimeoutError(f"Processing timed out after {seconds} seconds")

        signal.signal(signal.SIGALRM, handler)
        signal.alarm(seconds)
        try:
            yield
        finally:
            signal.alarm(0)

    def _get_error_response(self) -> Dict:
        """Return a structured error response."""
        return {
            "gevonden_delegitimatie": [],
            "samenvatting": {
                "aantal_gevallen": 0,
                "meest_voorkomende_type": "error",
                "meest_getroffen_doelgroep": "error",
                "ernst_score": 0,
                "gemiddelde_confidence": 0.0,
                "hoogste_confidence": 0.0,
                "laagste_confidence": 0.0
            }
        }
