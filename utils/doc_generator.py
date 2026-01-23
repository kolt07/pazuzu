# -*- coding: utf-8 -*-
"""
Модуль для генерації документації з Data Dictionary.
"""

from pathlib import Path
from typing import Dict, Any
from utils.data_dictionary import DataDictionary


class DocumentationGenerator:
    """Клас для генерації документації з Data Dictionary."""
    
    def __init__(self, data_dictionary: DataDictionary = None):
        """
        Ініціалізація генератора документації.
        
        Args:
            data_dictionary: Екземпляр Data Dictionary. Якщо None, створюється новий.
        """
        self.data_dictionary = data_dictionary or DataDictionary()
    
    def generate_markdown(self) -> str:
        """
        Генерує Markdown документацію з Data Dictionary.
        
        Returns:
            Markdown рядок з документацією
        """
        lines = []
        
        # Заголовок
        metadata = self.data_dictionary.get_metadata()
        lines.append("# Data Dictionary")
        lines.append("")
        lines.append(f"**Версія:** {metadata.get('version', 'N/A')}  ")
        lines.append(f"**Останнє оновлення:** {metadata.get('last_updated', 'N/A')}  ")
        lines.append("")
        lines.append(metadata.get('description', ''))
        lines.append("")
        lines.append("---")
        lines.append("")
        
        # Опис колекцій
        for collection_name in self.data_dictionary.list_collections():
            collection = self.data_dictionary.get_collection(collection_name)
            if not collection:
                continue
            
            lines.append(f"## Колекція: `{collection.mongo_collection}`")
            lines.append("")
            lines.append(f"**Назва:** {collection_name}  ")
            lines.append(f"**MongoDB колекція:** `{collection.mongo_collection}`  ")
            lines.append("")
            lines.append(f"**Опис:** {collection.description}")
            lines.append("")
            
            # Індекси
            if collection.indexes:
                lines.append("### Індекси")
                lines.append("")
                for index in collection.indexes:
                    field_name = index.get('field', '')
                    unique = index.get('unique', False)
                    description = index.get('description', '')
                    lines.append(f"- **{field_name}** {'(унікальний)' if unique else ''} - {description}")
                lines.append("")
            
            # Поля
            lines.append("### Поля")
            lines.append("")
            lines.append("| Назва | Тип | Обов'язкове | Опис | Одиниця | Приклад |")
            lines.append("|-------|-----|--------------|------|---------|---------|")
            
            for field_name, field_def in collection.fields.items():
                if field_def.mongo_generated:
                    continue  # Пропускаємо MongoDB згенеровані поля
                
                required = "✅" if field_def.required else "❌"
                unit = field_def.unit if field_def.unit else "-"
                example = field_def.example if field_def.example else "-"
                
                # Обрізаємо довгий опис
                description = field_def.description[:50] + "..." if len(field_def.description) > 50 else field_def.description
                
                lines.append(f"| `{field_name}` | {field_def.type} | {required} | {description} | {unit} | {example} |")
                
                # Додаємо інформацію про вкладені поля
                if field_def.nested_fields:
                    lines.append("")
                    lines.append(f"  **Вкладені поля `{field_name}`:**")
                    for nested_name, nested_def in field_def.nested_fields.items():
                        nested_required = "✅" if nested_def.required else "❌"
                        nested_unit = nested_def.unit if nested_def.unit else "-"
                        lines.append(f"  - `{nested_name}` ({nested_def.type}) {nested_required} - {nested_def.description} {nested_unit}")
                    lines.append("")
            
            lines.append("")
            
            # Enum значення
            for field_name, field_def in collection.fields.items():
                if field_def.enum:
                    lines.append(f"**Дозволені значення для `{field_name}`:**")
                    lines.append("")
                    for enum_value in field_def.enum:
                        lines.append(f"- `{enum_value}`")
                    lines.append("")
            
            # Зв'язки
            if collection.relationships:
                lines.append("### Зв'язки")
                lines.append("")
                for rel in collection.relationships:
                    rel_type = rel.get('type', '')
                    field = rel.get('field', '')
                    target_collection = rel.get('target_collection', '')
                    target_field = rel.get('target_field', '')
                    cardinality = rel.get('cardinality', '')
                    description = rel.get('description', '')
                    
                    lines.append(f"- **{rel_type}** через `{field}` → `{target_collection}.{target_field}` ({cardinality})")
                    lines.append(f"  - {description}")
                lines.append("")
            
            lines.append("---")
            lines.append("")
        
        return "\n".join(lines)
    
    def save_documentation(self, output_path: Path) -> None:
        """
        Зберігає документацію у файл.
        
        Args:
            output_path: Шлях до файлу для збереження
        """
        markdown = self.generate_markdown()
        
        # Створюємо директорію, якщо не існує
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(markdown)
    
    def generate_json_schema(self) -> Dict[str, Any]:
        """
        Генерує JSON Schema з Data Dictionary.
        
        Returns:
            Словник з JSON Schema
        """
        schema = {
            '$schema': 'http://json-schema.org/draft-07/schema#',
            'title': 'Pazuzu Data Dictionary',
            'type': 'object',
            'properties': {}
        }
        
        for collection_name in self.data_dictionary.list_collections():
            collection = self.data_dictionary.get_collection(collection_name)
            if not collection:
                continue
            
            collection_schema = {
                'type': 'object',
                'description': collection.description,
                'properties': {},
                'required': []
            }
            
            for field_name, field_def in collection.fields.items():
                if field_def.mongo_generated:
                    continue
                
                field_schema = {
                    'type': self._json_schema_type(field_def.type),
                    'description': field_def.description
                }
                
                if field_def.unit:
                    field_schema['unit'] = field_def.unit
                
                if field_def.example:
                    field_schema['example'] = field_def.example
                
                if field_def.enum:
                    field_schema['enum'] = field_def.enum
                
                if field_def.type == 'object' and field_def.nested_fields:
                    nested_properties = {}
                    nested_required = []
                    
                    for nested_name, nested_def in field_def.nested_fields.items():
                        nested_schema = {
                            'type': self._json_schema_type(nested_def.type),
                            'description': nested_def.description
                        }
                        if nested_def.unit:
                            nested_schema['unit'] = nested_def.unit
                        nested_properties[nested_name] = nested_schema
                        if nested_def.required:
                            nested_required.append(nested_name)
                    
                    field_schema['properties'] = nested_properties
                    if nested_required:
                        field_schema['required'] = nested_required
                
                collection_schema['properties'][field_name] = field_schema
                
                if field_def.required:
                    collection_schema['required'].append(field_name)
            
            schema['properties'][collection_name] = collection_schema
        
        return schema
    
    def _json_schema_type(self, field_type: str) -> str:
        """
        Конвертує тип поля в JSON Schema тип.
        
        Args:
            field_type: Тип поля
            
        Returns:
            JSON Schema тип
        """
        type_mapping = {
            'string': 'string',
            'number': 'number',
            'boolean': 'boolean',
            'datetime': 'string',  # JSON Schema не має datetime, використовуємо string
            'ObjectId': 'string',
            'object': 'object',
            'array': 'array'
        }
        
        return type_mapping.get(field_type, 'string')


def generate_documentation(output_dir: Path = None) -> None:
    """
    Генерує документацію з Data Dictionary.
    
    Args:
        output_dir: Директорія для збереження. Якщо None, використовується docs/
    """
    if output_dir is None:
        output_dir = Path(__file__).parent.parent / 'docs'
    
    generator = DocumentationGenerator()
    
    # Генеруємо Markdown
    md_path = output_dir / 'data_dictionary.md'
    generator.save_documentation(md_path)
    print(f"✓ Згенеровано Markdown документацію: {md_path}")
    
    # Генеруємо JSON Schema
    import json
    json_schema = generator.generate_json_schema()
    json_path = output_dir / 'data_dictionary.schema.json'
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(json_schema, f, indent=2, ensure_ascii=False)
    print(f"✓ Згенеровано JSON Schema: {json_path}")


if __name__ == "__main__":
    generate_documentation()
