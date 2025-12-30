"""Intent-based token-efficient tool handling for the company research agent.

This module provides a system where the LLM decides per tool call whether it needs
full data for analysis or just a placeholder for pass-through/enrichment.
"""

import json
import re
from typing import Any, Dict, List, Optional, Set, Tuple


class IntentBasedTokenHandler:
    """Handles token-efficient tool responses based on LLM's intent per call."""

    def __init__(self):
        """Initialize the handler."""
        # Storage for placeholder -> actual data mapping
        self.placeholder_data: Dict[str, Any] = {}

    def is_placeholder_response(self, tool_response: Any) -> bool:
        """Check if a tool response contains a placeholder."""
        if isinstance(tool_response, dict) and "placeholder" in tool_response:
            return True
        return False

    def extract_placeholder_data(
            self, tool_response: Dict[str, Any]) -> Tuple[str, Any]:
        """Extract placeholder ID and store any embedded data.

        Args:
            tool_response: Response from a tool that used placeholder mode

        Returns:
            Tuple of (placeholder_text, stored_data)
        """
        placeholder = tool_response.get("placeholder", "")

        # If the tool already stored the data internally, we just track the placeholder
        # The actual data retrieval will happen during post-processing
        return placeholder, tool_response

    def create_contextual_response(self, tool_name: str,
                                   tool_response: Any) -> str:
        """Create an appropriate response based on whether it's a placeholder or full data.

        Args:
            tool_name: Name of the tool
            tool_response: The tool's response

        Returns:
            A response string appropriate for the LLM
        """
        if self.is_placeholder_response(tool_response):
            # Tool returned placeholder mode
            placeholder = tool_response.get("placeholder", "")
            summary = tool_response.get("summary", "Data retrieved")

            # Store the placeholder reference
            self.placeholder_data[placeholder] = tool_response

            return (f"{summary}. "
                    f"Data reference: {placeholder}")
        else:
            # Tool returned full data - return as normal
            return str(tool_response)

    def replace_placeholders(self, text: str) -> str:
        """Replace all placeholders in text with actual data.

        This method looks for placeholders and retrieves the actual data
        from the tools' internal storage.

        Args:
            text: Text containing placeholders

        Returns:
            Text with placeholders replaced by actual data
        """
        # Import here to avoid circular imports
        from fusionbase.ai.tools.relation.convenience_v2 import get_placeholder_data

        # Handle both string and structured outputs
        if isinstance(text, str):
            return self._replace_placeholders_in_string(text,
                                                        get_placeholder_data)
        elif isinstance(text, dict):
            return self._replace_placeholders_in_dict(text,
                                                      get_placeholder_data)
        elif isinstance(text, list):
            return self._replace_placeholders_in_list(text,
                                                      get_placeholder_data)
        else:
            return text

    def _replace_placeholders_in_string(self, text: str, data_retriever) -> str:
        """Replace placeholders in a string."""
        result = text

        # Find all placeholders in the text (format: <tool_name_hash>)
        placeholder_pattern = r'<([a-zA-Z_]+_[a-f0-9]{8})>'
        placeholders = re.findall(placeholder_pattern, result)

        for placeholder_id in placeholders:
            # Get the actual data from the tool's storage
            full_data = data_retriever(placeholder_id)

            if full_data:
                # Convert data to appropriate string format
                if isinstance(full_data, (dict, list)):
                    # For JSON-like data, pretty print it
                    data_str = json.dumps(full_data,
                                          indent=2,
                                          ensure_ascii=False)
                else:
                    data_str = str(full_data)

                # Replace the placeholder
                result = result.replace(f"<{placeholder_id}>", data_str)

        return result

    def _replace_placeholders_in_dict(self, obj: Dict[str, Any],
                                      data_retriever) -> Dict[str, Any]:
        """Recursively replace placeholders in a dictionary."""
        result = {}
        for key, value in obj.items():
            if isinstance(value, str):
                # Check if it's a placeholder
                if value.startswith("<") and value.endswith(">"):
                    placeholder_id = value[1:-1]
                    full_data = data_retriever(placeholder_id)
                    if full_data:
                        result[key] = full_data
                    else:
                        # Try string replacement
                        result[key] = self._replace_placeholders_in_string(
                            value, data_retriever)
                else:
                    # Check for embedded placeholders
                    result[key] = self._replace_placeholders_in_string(
                        value, data_retriever)
            elif isinstance(value, dict):
                result[key] = self._replace_placeholders_in_dict(
                    value, data_retriever)
            elif isinstance(value, list):
                result[key] = self._replace_placeholders_in_list(
                    value, data_retriever)
            else:
                result[key] = value
        return result

    def _replace_placeholders_in_list(self, obj: List[Any],
                                      data_retriever) -> List[Any]:
        """Recursively replace placeholders in a list."""
        result = []
        for item in obj:
            if isinstance(item, str):
                # Check if it's a placeholder
                if item.startswith("<") and item.endswith(">"):
                    placeholder_id = item[1:-1]
                    full_data = data_retriever(placeholder_id)
                    if full_data:
                        result.append(full_data)
                    else:
                        result.append(
                            self._replace_placeholders_in_string(
                                item, data_retriever))
                else:
                    result.append(
                        self._replace_placeholders_in_string(
                            item, data_retriever))
            elif isinstance(item, dict):
                result.append(
                    self._replace_placeholders_in_dict(item, data_retriever))
            elif isinstance(item, list):
                result.append(
                    self._replace_placeholders_in_list(item, data_retriever))
            else:
                result.append(item)
        return result

    def clear(self):
        """Clear stored placeholder data."""
        # Also clear the tool's internal storage
        from fusionbase.ai.tools.relation.convenience_v2 import clear_placeholder_store
        clear_placeholder_store()

        self.placeholder_data.clear()
