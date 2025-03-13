"""
Score Extraction Agent

This module implements an agent using Claude 3.7 to extract scores from HTML when
standard extraction methods fail.
"""

import os
import json
import anthropic
from typing import Dict, List, Any, Optional, Tuple
from scrapy.http import Response
from scrapy import Selector
from scrapy.selector import SelectorList
import logging

class ScoreExtractionAgent:
    """
    Agent that uses Claude to extract scores from HTML when standard methods fail.
    """
    
    def __init__(self, api_key=None):
        """
        Initialize the score extraction agent.
        
        Args:
            api_key: Anthropic API key (optional, will look for ANTHROPIC_API_KEY env var)
        """
        self.api_key = api_key or os.environ.get("ANTHROPIC_API_KEY")
        if not self.api_key:
            raise ValueError("Anthropic API key not provided and ANTHROPIC_API_KEY environment variable not set")
        
        self.client = anthropic.Anthropic(api_key=self.api_key)
        self.logger = logging.getLogger(__name__)
    
    def get_tools(self):
        """
        Define the tools available to the agent using the Anthropic API format.
        """
        return [
            {
                "name": "extract_scores_from_html",
                "description": "Extract home and away scores from a row in an HTML table.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "html_row": {
                            "type": "string",
                            "description": "The HTML of a single row from a schedule table"
                        }
                    },
                    "required": ["html_row"]
                }
            },
            {
                "name": "analyze_table_structure",
                "description": "Analyze the structure of the HTML table to determine where scores might be located.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "html_table": {
                            "type": "string",
                            "description": "The HTML of the entire schedule table"
                        }
                    },
                    "required": ["html_table"]
                }
            }
        ]
    
    def get_tool_mapping(self):
        """
        Maps tool names to their implementation functions.
        """
        return {
            "extract_scores_from_html": self.extract_scores_from_html,
            "analyze_table_structure": self.analyze_table_structure
        }
    
    def extract_scores_from_html(self, params: Dict) -> Dict:
        """
        Extract scores from a row of HTML.
        
        Args:
            params: Dict containing the HTML row
            
        Returns:
            Dict with extracted scores or error
        """
        try:
            html_row = params.get("html_row")
            if not html_row:
                return {"error": "No HTML row provided"}
            
            # Create a Scrapy selector from the HTML row
            row_selector = Selector(text=html_row)
            
            # Look for spans with score-like content
            spans = row_selector.css('span::text').getall()
            score_text = None
            
            # Check spans for score-like text
            for span in spans:
                span_text = span.strip()
                if ' - ' in span_text and len(span_text) < 10:  # Heuristic for score format
                    try:
                        parts = span_text.split(' - ')
                        if len(parts) == 2:
                            left = parts[0].strip()
                            right = parts[1].strip()
                            if left.isdigit() and right.isdigit():
                                score_text = span_text
                                break
                    except Exception:
                        continue
            
            if score_text:
                scores = score_text.split(' - ')
                return {
                    "result": {
                        "home_score": int(scores[0].strip()),
                        "away_score": int(scores[1].strip()),
                        "score_text": score_text
                    }
                }
            
            # If no score found in spans, try looking at td content directly
            cells = row_selector.css('td')
            for cell in cells:
                cell_text = cell.css('::text').get('').strip()
                if ' - ' in cell_text and len(cell_text) < 10:
                    try:
                        scores = cell_text.split(' - ')
                        if len(scores) == 2:
                            left = scores[0].strip()
                            right = scores[1].strip()
                            if left.isdigit() and right.isdigit():
                                return {
                                    "result": {
                                        "home_score": int(left),
                                        "away_score": int(right),
                                        "score_text": cell_text
                                    }
                                }
                    except Exception:
                        continue
            
            return {"result": {"home_score": None, "away_score": None, "score_text": None, "message": "No scores found in row"}}
        except Exception as e:
            return {"error": str(e)}
    
    def analyze_table_structure(self, params: Dict) -> Dict:
        """
        Analyze the structure of the HTML table to determine where scores might be located.
        
        Args:
            params: Dict containing the HTML table
            
        Returns:
            Dict with analysis results or error
        """
        try:
            html_table = params.get("html_table")
            if not html_table:
                return {"error": "No HTML table provided"}
            
            # Create a Scrapy selector from the HTML table
            table_selector = Selector(text=html_table)
            
            # Look for table headers to understand the column structure
            headers = table_selector.css('th::text').getall()
            header_titles = [h.strip() for h in headers if h.strip()]
            
            # Analyze the first few rows to identify potential score columns
            rows = table_selector.css('tr')
            if len(rows) <= 1:  # Only header row
                return {"result": {"message": "Table has no data rows", "headers": header_titles}}
            
            sample_rows = rows[1:min(4, len(rows))]  # Get up to 3 data rows
            score_column_index = -1
            score_column_classes = []
            
            # Check each column for score-like content
            for row in sample_rows:
                cells = row.css('td')
                for i, cell in enumerate(cells):
                    cell_text = cell.css('::text').get('').strip()
                    if ' - ' in cell_text:
                        try:
                            parts = cell_text.split(' - ')
                            if len(parts) == 2 and parts[0].strip().isdigit() and parts[1].strip().isdigit():
                                score_column_index = i
                                if 'class' in cell.attrib:
                                    score_column_classes.append(cell.attrib['class'])
                                break
                        except Exception:
                            continue
                    
                    # Check for scores in spans within the cell
                    spans = cell.css('span::text').getall()
                    for span in spans:
                        span_text = span.strip()
                        if ' - ' in span_text:
                            try:
                                parts = span_text.split(' - ')
                                if len(parts) == 2 and parts[0].strip().isdigit() and parts[1].strip().isdigit():
                                    score_column_index = i
                                    if 'class' in cell.attrib:
                                        score_column_classes.append(cell.attrib['class'])
                                    break
                            except Exception:
                                continue
            
            return {
                "result": {
                    "header_titles": header_titles,
                    "score_column_index": score_column_index,
                    "score_column_classes": score_column_classes,
                    "has_versus_column": "versus" in " ".join(header_titles).lower() or any('versus' in cls.lower() for cls in score_column_classes)
                }
            }
        except Exception as e:
            return {"error": str(e)}
    
    def extract_scores(self, row_html=None, table_html=None, row_selector=None, table_selector=None):
        """
        Main method to extract scores using the agent.
        
        Args:
            row_html: HTML of a single row (optional)
            table_html: HTML of the entire table (optional)
            row_selector: Scrapy Selector for a row (optional)
            table_selector: Scrapy Selector for a table (optional)
            
        Returns:
            Tuple of (home_score, away_score, extraction_method)
        """
        # Convert selectors to HTML if provided
        if row_selector and not row_html:
            row_html = row_selector.get()
        if table_selector and not table_html:
            table_html = table_selector.get()
        
        # Try local extraction methods first
        if row_html:
            result = self.extract_scores_from_html({"html_row": row_html})
            if result.get("result") and result["result"].get("home_score") is not None:
                return (result["result"]["home_score"], 
                        result["result"]["away_score"], 
                        "local_extraction")
        
        # If local methods fail, use Claude API
        system_prompt = """
        <purpose>
            You are a specialized web scraping assistant that extracts football/soccer game scores from HTML.
        </purpose>
        
        <instructions>
            <task>
                Your task is to identify and extract the home and away scores from an HTML row 
                representing a football/soccer game.
            </task>
            
            <data_format>
                The scores are typically formatted as "X - Y" where X is the home score and Y is the away score.
                This might be in a cell with class="schedule-versus-column" or similar.
                The scores are often inside a <span> element but might be directly in a table cell.
            </data_format>
            
            <response_format>
                Only respond with the extracted scores. If you can't find scores, explain why.
                Always format your answer as a valid JSON with home_score and away_score fields.
            </response_format>
        </instructions>
        """
        
        messages = []
        
        # Start with table analysis if available
        if table_html:
            messages.append({
                "role": "user", 
                "content": [{"type": "text", "text": "Analyze this HTML table to find where scores are located:"}]
            })
            
            # Use Claude's tool calling to analyze the table
            response = self.client.messages.create(
                model="claude-3-sonnet-20240229",
                max_tokens=1000,
                messages=messages,
                system=system_prompt,
                tools=self.get_tools(),
                temperature=0.0
            )
            
            tool_calls = [block for block in response.content if block.type == "tool_use"]
            if tool_calls:
                for tool in tool_calls:
                    if tool.name == "analyze_table_structure":
                        func = self.get_tool_mapping().get(tool.name)
                        output = func(tool.input)
                        
                        # Add the tool result to the messages
                        messages.append({
                            "role": "assistant",
                            "content": response.content
                        })
                        messages.append({
                            "role": "user",
                            "content": [{
                                "type": "tool_result",
                                "tool_use_id": tool.id,
                                "content": json.dumps(output)
                            }]
                        })
        
        # Now process the row
        if row_html:
            messages.append({
                "role": "user", 
                "content": [{"type": "text", "text": "Extract the home and away scores from this HTML row:"}]
            })
            
            # Use Claude's tool calling for row extraction
            response = self.client.messages.create(
                model="claude-3-sonnet-20240229",
                max_tokens=1000,
                messages=messages,
                system=system_prompt,
                tools=self.get_tools(),
                temperature=0.0
            )
            
            tool_calls = [block for block in response.content if block.type == "tool_use"]
            if tool_calls:
                for tool in tool_calls:
                    if tool.name == "extract_scores_from_html":
                        func = self.get_tool_mapping().get(tool.name)
                        output = func(tool.input)
                        
                        if "result" in output and output["result"].get("home_score") is not None:
                            return (output["result"]["home_score"], 
                                    output["result"]["away_score"], 
                                    "agent_extraction")
            
            # If tool calls didn't work, try getting a direct text response
            response_text = ""
            for block in response.content:
                if block.type == "text":
                    response_text += block.text
            
            # Try to parse JSON response
            try:
                # Handle cases where Claude might wrap the JSON in ```json
                json_text = response_text
                if "```json" in json_text:
                    json_text = json_text.split("```json")[1].split("```")[0].strip()
                elif "```" in json_text:
                    json_text = json_text.split("```")[1].split("```")[0].strip()
                
                result = json.loads(json_text)
                if "home_score" in result and "away_score" in result:
                    home_score = result["home_score"]
                    away_score = result["away_score"]
                    if home_score is not None and away_score is not None:
                        try:
                            home_score = int(home_score)
                            away_score = int(away_score)
                            return home_score, away_score, "claude_direct_response"
                        except (ValueError, TypeError):
                            pass
            except Exception as e:
                self.logger.warning(f"Failed to parse Claude response as JSON: {e}")
                
        # If we got here, no scores were found
        return None, None, "no_scores_found"
