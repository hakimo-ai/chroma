import ast
import inspect
import os.path
import traceback
from types import FrameType
from typing import Any, Dict, cast, get_args, Union, Optional

from chromadb.types import (
    WhereOperator,
    InclusionExclusionOperator,
    LiteralValue,
    Where,
)


def _map_ast_operator_to_where_operator(
    operator: Union[
        ast.Eq, ast.NotEq, ast.In, ast.NotIn, ast.Gt, ast.GtE, ast.Lt, ast.LtE
    ]
) -> Union[WhereOperator, InclusionExclusionOperator]:
    if isinstance(operator, ast.Eq):
        return "$eq"
    elif isinstance(operator, ast.NotEq):
        return "$ne"
    elif isinstance(operator, ast.Gt):
        return "$gt"
    elif isinstance(operator, ast.GtE):
        return "$gte"
    elif isinstance(operator, ast.Lt):
        return "$lt"
    elif isinstance(operator, ast.LtE):
        return "$lte"
    elif isinstance(operator, ast.In):
        return "$in"
    elif isinstance(operator, ast.NotIn):
        return "$nin"
    else:
        raise ValueError(f"Unsupported operator: {operator}")


def process_ast_compare(node: ast.Compare) -> Dict[str, Any]:
    if isinstance(node.left, ast.Name):
        left = node.left.id
    elif isinstance(node.left, ast.Attribute):
        left = node.left.attr
    elif isinstance(node.left, ast.Str):
        left = node.left.s
    else:
        raise ValueError(
            f"Unsupported left hand side type: {type(node.left)}. Must be a string."
        )
    operator = node.ops[0]
    # print(f"operator: {operator}")
    right = node.comparators[0]
    if not isinstance(
        operator,
        (ast.Eq, ast.NotEq, ast.In, ast.NotIn, ast.Gt, ast.GtE, ast.Lt, ast.LtE),
    ):
        raise ValueError(f"Unsupported operator: {operator}")
    if isinstance(right, (ast.Str, ast.Num, ast.Constant)):
        right_value = right.value
    elif isinstance(right, ast.List):
        right_value = [_process_ast(value) for value in right.elts]
    elif isinstance(right, ast.UnaryOp):
        if isinstance(right.op, ast.USub) and isinstance(right.operand, ast.Num):
            right_value = f"-{right.operand.n}"
        else:
            raise ValueError(
                f"Unsupported right hand side type: {type(right)}. Must be a string or a list of strings."
            )
    else:
        raise ValueError(
            f"Unsupported right hand side type: {type(right)}. Must be a string or a list of strings."
        )
    if isinstance(
        operator,
        (ast.Eq, ast.NotEq, ast.In, ast.NotIn, ast.Gt, ast.GtE, ast.Lt, ast.LtE),
    ):
        return {
            f"{left}": {f"{_map_ast_operator_to_where_operator(operator)}": right_value}
        }
    else:
        raise ValueError(
            f"Unsupported right hand side type: {type(right)}. Must be a string or a list of strings."
        )


def _process_ast(node: Any) -> Union[LiteralValue, Dict[str, Any]]:
    # print(f"Processing node: {node}")
    if isinstance(node, ast.BoolOp):
        if isinstance(node.op, ast.And):
            return {"$and": [_process_ast(value) for value in node.values]}
        elif isinstance(node.op, ast.Or):
            return {"$or": [_process_ast(value) for value in node.values]}
    elif isinstance(node, ast.Compare):
        return process_ast_compare(node)
    elif isinstance(node, ast.Module):
        return _process_ast(node.body[0])
    elif isinstance(node, ast.Expr):
        return _process_ast(node.value)
    elif isinstance(node, get_args(LiteralValue)) or isinstance(node, ast.Constant):
        return cast(LiteralValue, node.value)
    # elif isinstance(node, ) and isinstance(
    #     node.value, get_args(LiteralValue)
    # ):
    #     return cast(LiteralValue, node.value)
    elif isinstance(node, ast.UnaryOp):
        if isinstance(node.op, ast.USub) and isinstance(node.operand, ast.Num):
            return f"-{node.operand.n}"
        else:
            raise ValueError(
                f"Unsupported right hand side type: {type(node)}. Must be a string or a list of strings."
            )
    raise ValueError(f"Unsupported node type: {type(node)}")


def extract_conditions_text(
    line: str, where_function_name: str = "Filter.where"
) -> str:
    start = line[line.find(where_function_name) + len(where_function_name) :]
    condition = ""
    stack = []
    for c in start:
        if c == "(":
            stack.append(c)
        elif c == ")" and stack:
            stack.pop()
        if len(stack) > 0:
            condition += c
        if len(stack) == 0:
            break
    if len(stack) > 0:
        raise ValueError(
            "Invalid expression. For multi-line expression, "
            "use quotes to wrap your expression. The environment you're using "
            "does not support pure python expressions parsing from source (e.g. Jupyter notebook)."
        )
    condition = condition[1:]
    return condition


def extract_conditions_fame(
    frame: Optional[FrameType], where_function_name: str
) -> str:
    if not frame:
        raise ValueError("Unable to extract conditions from the current frame.")
    filename = frame.f_code.co_filename
    lineno = frame.f_lineno

    with open(filename, "r") as f:
        lines = f.readlines()

    condition = ""
    stack = []
    for li, line in enumerate(lines[lineno - 1 :]):
        if li == 0:
            line = line[line.find(where_function_name) :]
        for c in line:
            if c == "(":
                stack.append(c)
            elif c == ")":
                if stack:
                    stack.pop()
            if len(stack) > 0:
                condition += c

        if len(stack) == 0:
            break
    if where_function_name in condition:
        return (
            condition[len(where_function_name) + 2 :][:-1]
            .replace("\n", "")
            .replace("  ", "")
        )
    else:
        return condition[1:].replace("\n", "").replace("  ", "")


class Filter(object):
    @staticmethod
    def where(e: Union[str, bool]) -> Where:
        if isinstance(e, str):
            _filter_expr = _process_ast(ast.parse(e))
        else:
            stack = traceback.extract_stack()
            filename, _, _, text = stack[-2]
            if os.path.exists(filename):
                frame = inspect.currentframe()
                _expr = extract_conditions_fame(
                    frame.f_back if frame else None, "Filter.where"
                )
            else:
                _expr = extract_conditions_text(text)
            _filter_expr = _process_ast(ast.parse(_expr))
        return cast(Where, _filter_expr)