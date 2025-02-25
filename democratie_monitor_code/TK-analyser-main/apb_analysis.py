import pandas as pd
import json
from typing import Dict, List, Optional
import os
from datetime import datetime
import logging
from tqdm import tqdm
import yaml
import argparse
from fireworks_processor import FireworksProcessor
import glob

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PromptTemplate:
    def __init__(self, template_path: str):
        """Load prompt template from YAML file."""
        with open(template_path, 'r', encoding='utf-8') as f:
            template_data = yaml.safe_load(f)
            self.template = template_data['prompt_template']

    def format(self, **kwargs) -> str:
        """Format the template with provided arguments."""
        return self.template.format(**kwargs)

class APBSpeechAnalyzer:
    def __init__(self, csv_path: str, processor: FireworksProcessor, prompt_template: PromptTemplate):
        """Initialize the APB speech analyzer."""
        self.csv_path = csv_path
        self.processor = processor
        self.prompt_template = prompt_template
        self.df = None

    def load_data(self) -> None:
        """Load and preprocess the APB speeches data."""
        try:
            # Get all speech CSV files
            speech_files = glob.glob(os.path.join(self.csv_path, 'speeches_*.csv'))

            if not speech_files:
                # Try as single file if no matching pattern found
                if os.path.isfile(self.csv_path):
                    speech_files = [self.csv_path]
                else:
                    raise FileNotFoundError(f"No speech files found at {self.csv_path}")

            # Load and combine all CSVs
            all_speeches = []
            for file_path in speech_files:
                logger.info(f"Loading speeches from {file_path}")
                df = pd.read_csv(file_path)
                # Add source file information
                df['source_file'] = os.path.basename(file_path)
                all_speeches.append(df)

            # Concatenate all dataframes
            self.df = pd.concat(all_speeches, ignore_index=True)

            # Convert date column to datetime
            self.df['date'] = pd.to_datetime(self.df['date'])
            logger.info(f"Loaded {len(self.df)} speeches from {len(speech_files)} files")

        except Exception as e:
            logger.error(f"Error loading data: {e}")
            raise

    def analyze_speech(self, speech_text: str, source_file: str) -> Dict:
        """Analyze delegitimation patterns in a speech."""
        try:
            formatted_prompt = self.prompt_template.format(text=speech_text)
            result = self.processor.process_text(
                text=speech_text,
                prompt=formatted_prompt,
                source_file=source_file,
                original_text=speech_text
            )
            if not result or not isinstance(result, dict):
                logger.error(f"Invalid result type: {type(result)}")
                return None
            return result
        except Exception as e:
            logger.error(f"Error analyzing speech: {str(e)}")
            logger.error(f"Error type: {type(e)}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return None

    def run_sample_analysis(self, n_samples: int = 10, min_length: int = 50) -> Dict:
        """Run analysis on a random sample of speeches."""
        if self.df is None:
            self.load_data()

        # Filter out short speeches
        valid_speeches = self.df[self.df['speech_text'].str.len() >= min_length]
        logger.info(f"Found {len(valid_speeches)} speeches with length >= {min_length} characters")

        if len(valid_speeches) < n_samples:
            logger.warning(f"Only {len(valid_speeches)} valid speeches available, reducing sample size")
            n_samples = len(valid_speeches)

        # Sample random speeches
        sample_df = valid_speeches.sample(n=n_samples, random_state=42)

        # Analyze each sampled speech
        sample_analyses = {}
        for idx, row in tqdm(sample_df.iterrows(), total=n_samples, desc="Analyzing sample speeches"):
            text_length = len(row['speech_text'])
            logger.info(f"Analyzing speech by {row['speaker_name']} ({text_length} characters)")

            try:
                # Pass the source_file from the DataFrame
                analysis_result = self.analyze_speech(
                    row['speech_text'],
                    source_file=row['source_file']  # Pass the source file
                )
                if not analysis_result:
                    continue

                speech_analysis = {
                    'speaker_name': row['speaker_name'],
                    'speaker_party': row['speaker_party'],
                    'date': row['date'].strftime('%Y-%m-%d'),
                    'text_length': text_length,
                    'source_file': row['source_file'],  # Include source file in output
                    'analysis': analysis_result
                }
                sample_analyses[str(idx)] = speech_analysis
                logger.info(f"Successfully analyzed speech by {row['speaker_name']}")

            except Exception as e:
                logger.error(f"Error analyzing speech by {row['speaker_name']}: {str(e)}")
                continue

        if not sample_analyses:
            logger.warning("No successful analyses completed")
            return {"error": "No successful analyses", "analyses": {}}

        return {
            "status": "success",
            "metadata": {
                "min_length": min_length,
                "requested_samples": n_samples,
                "successful_analyses": len(sample_analyses)
            },
            "analyses": sample_analyses
        }

    def run_full_analysis(self, output_dir: str) -> None:
        """Run a comprehensive analysis of all speeches."""
        os.makedirs(output_dir, exist_ok=True)

        # Overall statistics
        stats = {
            'total_speeches': len(self.df),
            'unique_parties': self.df['speaker_party'].nunique(),
            'date_range': f"{self.df['date'].min()} to {self.df['date'].max()}",
            'speeches_per_party': self.df['speaker_party'].value_counts().to_dict()
        }

        # Analyze each party's speeches
        parties = self.df['speaker_party'].unique()
        party_analyses = {}

        for party in tqdm(parties, desc="Analyzing parties"):
            party_speeches = self.df[self.df['speaker_party'] == party]
            for _, speech in party_speeches.iterrows():
                combined_text = speech['speech_text']
                formatted_prompt = self.prompt_template.format(text=combined_text)
                result = self.processor.process_text(
                    text=combined_text,
                    prompt=formatted_prompt,
                    source_file=speech['source_file'],  # Pass the source file
                    original_text=combined_text
                )
                if party not in party_analyses:
                    party_analyses[party] = []
                party_analyses[party].append({
                    'source_file': speech['source_file'],
                    'analysis': result
                })

        # Save results
        results = {
            'statistics': stats,
            'party_analyses': party_analyses
        }

        output_file = os.path.join(output_dir, f"analysis_{datetime.now().strftime('%Y%m%d')}.json")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)

        logger.info(f"Analysis complete. Results saved to {output_file}")

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Analyze APB speeches for delegitimation patterns')
    parser.add_argument('--csv_path', type=str, default='data/apb/apb_speeches.csv',
                      help='Path to the CSV file containing speeches')
    parser.add_argument('--prompt_path', type=str, default='prompts/delegitimatie.yaml',
                      help='Path to the YAML file containing the prompt template')
    parser.add_argument('--output_dir', type=str, default='analysis_output',
                      help='Directory to save analysis results')
    parser.add_argument('--sample', type=int, default=0,
                      help='Number of random speeches to analyze (0 for full analysis)')
    parser.add_argument('--min-length', type=int, default=50,
                      help='Minimum text length to analyze')
    args = parser.parse_args()

    # Configuration
    api_key = os.getenv("FIREWORKS_API_KEY")
    if not api_key:
        raise ValueError("Please set FIREWORKS_API_KEY environment variable")

    # Initialize components
    prompt_template = PromptTemplate(args.prompt_path)
    processor = FireworksProcessor(api_key)
    apb_analyzer = APBSpeechAnalyzer(args.csv_path, processor, prompt_template)

    # Run analysis
    try:
        apb_analyzer.load_data()

        if args.sample > 0:
            # Run sample analysis
            sample_results = apb_analyzer.run_sample_analysis(
                n_samples=args.sample,
                min_length=args.min_length
            )

            # Save sample results
            sample_output_file = os.path.join(args.output_dir, f"sample_analysis_{datetime.now().strftime('%Y%m%d')}.json")
            os.makedirs(args.output_dir, exist_ok=True)

            with open(sample_output_file, 'w', encoding='utf-8') as f:
                json.dump(sample_results, f, ensure_ascii=False, indent=2)

            logger.info(f"Sample analysis completed. Results saved to {sample_output_file}")
        else:
            # Run full analysis
            apb_analyzer.run_full_analysis(args.output_dir)
            logger.info("Full analysis completed successfully")

    except Exception as e:
        logger.error(f"Analysis failed: {e}")
        raise

if __name__ == "__main__":
    main()
