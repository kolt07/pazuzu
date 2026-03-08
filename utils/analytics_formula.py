# -*- coding: utf-8 -*-
"""
Парсер формул для обчислення метрик аналітики.
Дозволяє агенту задавати метрики виразами над полями документів (auction_data.*, llm_result.result.*).
Безпечно: лише шляхи до полів, числа та оператори +, -, *, /; без виконання довільного коду.
"""

import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Union

# Дозволені префікси шляхів (захист від доступу до довільних полів)
ALLOWED_PATH_PREFIXES = ('auction_data.', 'llm_result.')


@dataclass
class PathNode:
    """Шлях до поля в документі, напр. auction_data.value.amount."""
    path: str  # dot-separated

    def to_mongo(self) -> Dict[str, Any]:
        # Конвертуємо рядкові числа в double для коректних обчислень
        return {
            '$convert': {
                'input': f'${self.path}',
                'to': 'double',
                'onError': None,
                'onNull': None
            }
        }


@dataclass
class NumberNode:
    value: float

    def to_mongo(self) -> Dict[str, Any]:
        return self.value


@dataclass
class BinOpNode:
    op: str  # +, -, *, /
    left: Union['PathNode', 'NumberNode', 'BinOpNode']
    right: Union['PathNode', 'NumberNode', 'BinOpNode']

    def to_mongo(self) -> Dict[str, Any]:
        left_expr = self.left.to_mongo()
        right_expr = self.right.to_mongo()
        if self.op == '+':
            return {'$add': [left_expr, right_expr]}
        if self.op == '-':
            return {'$subtract': [left_expr, right_expr]}
        if self.op == '*':
            return {'$multiply': [left_expr, right_expr]}
        if self.op == '/':
            # Безпечне ділення: null якщо дільник 0 або null
            return {
                '$cond': {
                    'if': {
                        '$and': [
                            {'$ne': [right_expr, None]},
                            {'$ne': [right_expr, 0]}
                        ]
                    },
                    'then': {'$divide': [left_expr, right_expr]},
                    'else': None
                }
            }
        raise ValueError(f"Невідомий оператор: {self.op}")


Node = Union[PathNode, NumberNode, BinOpNode]


class FormulaParseError(Exception):
    """Помилка парсингу формули."""
    pass


class FormulaParser:
    """Парсер формул: шлях, число, бінарні оператори з пріоритетом * / перед + -."""

    def __init__(self, formula: str):
        self.formula = formula.strip()
        self._tokens: List[Tuple[str, Any]] = []
        self._pos = 0

    def _tokenize(self) -> None:
        """Токенізація: path, number, +, -, *, /, (, )."""
        s = self.formula
        self._tokens = []
        i = 0
        while i < len(s):
            if s[i].isspace():
                i += 1
                continue
            if s[i] in '()+*-/':
                self._tokens.append((s[i], s[i]))
                i += 1
                continue
            # Число
            if s[i].isdigit() or (s[i] == '.' and i + 1 < len(s) and s[i + 1].isdigit()):
                start = i
                if s[i] == '.':
                    i += 1
                while i < len(s) and (s[i].isdigit() or s[i] == '.'):
                    i += 1
                try:
                    num = float(s[start:i])
                    self._tokens.append(('number', num))
                except ValueError:
                    raise FormulaParseError(f"Невірне число: {s[start:i]}")
                continue
            # Ідентифікатор або шлях (identifier.identifier...)
            if s[i].isalpha() or s[i] == '_':
                start = i
                while i < len(s) and (s[i].isalnum() or s[i] == '_' or s[i] == '.'):
                    i += 1
                path = s[start:i]
                if not path.replace('.', '').replace('_', '').isalnum():
                    raise FormulaParseError(f"Невірний шлях: {path}")
                # Дозволені лише шляхи з auction_data.* або llm_result.*
                if path.startswith(ALLOWED_PATH_PREFIXES[0]) or path.startswith(ALLOWED_PATH_PREFIXES[1]):
                    self._tokens.append(('path', path))
                elif path in ('auction_data', 'llm_result'):
                    raise FormulaParseError(f"Вкажіть повний шлях, напр. {path}.value.amount або {path}.result.building_area_sqm")
                else:
                    raise FormulaParseError(
                        f"Шлях '{path}' має починатися з auction_data. або llm_result."
                    )
                continue
            raise FormulaParseError(f"Невідомий символ: {s[i]!r}")
        self._tokens.append(('eof', None))

    def _cur(self) -> Tuple[str, Any]:
        if self._pos >= len(self._tokens):
            return ('eof', None)
        return self._tokens[self._pos]

    def _advance(self) -> Tuple[str, Any]:
        t = self._cur()
        self._pos += 1
        return t

    def _parse_primary(self) -> Node:
        kind, val = self._cur()
        if kind == 'number':
            self._advance()
            return NumberNode(val)
        if kind == 'path':
            self._advance()
            return PathNode(val)
        if kind == '(':
            self._advance()
            expr = self._parse_expression()
            if self._cur()[0] != ')':
                raise FormulaParseError("Очікується ')'")
            self._advance()
            return expr
        raise FormulaParseError(f"Очікується шлях, число або '(': {kind!r}")

    def _parse_term(self) -> Node:
        left = self._parse_primary()
        while self._cur()[0] in ('*', '/'):
            op = self._advance()[1]
            right = self._parse_primary()
            left = BinOpNode(op, left, right)
        return left

    def _parse_expression(self) -> Node:
        left = self._parse_term()
        while self._cur()[0] in ('+', '-'):
            op = self._advance()[1]
            right = self._parse_term()
            left = BinOpNode(op, left, right)
        return left

    def parse(self) -> Node:
        self._tokenize()
        if self._cur()[0] == 'eof':
            raise FormulaParseError("Формула не може бути порожньою")
        node = self._parse_expression()
        if self._cur()[0] != 'eof':
            raise FormulaParseError(f"Зайві символи після виразу: {self._cur()!r}")
        return node


def parse_formula(formula: str) -> Node:
    """
    Парсить рядок формули в AST.
    
    Дозволено: шляхи (auction_data.*, llm_result.*), числа, оператори +, -, *, /, дужки.
    Приклад: auction_data.value.amount / llm_result.result.building_area_sqm
    """
    return FormulaParser(formula).parse()


def formula_to_mongo_expr(formula: str) -> Dict[str, Any]:
    """
    Перетворює формулу в вираз для MongoDB $addFields.
    
    Returns:
        Словник, придатний для _metric_value у aggregation pipeline.
    """
    node = parse_formula(formula)
    return node.to_mongo()


def formula_references_llm(formula: str) -> bool:
    """Перевіряє, чи формула звертається до llm_result (потрібен $lookup llm_cache)."""
    return 'llm_result' in formula


def formula_hash(formula: str) -> str:
    """Повертає стабільний хеш формули для кешування (нормалізація пробілів)."""
    import hashlib
    normalized = ' '.join(formula.split())
    return hashlib.sha256(normalized.encode('utf-8')).hexdigest()[:16]
