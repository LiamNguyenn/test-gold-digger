"""Common utils for common use."""
import re


def common_string_to_snake_case(value: str) -> str:
    """
    transform from normal string to follow snake case.

    @param value: string need to transform
    @return: string of snake case converted
    """
    snake_case_str = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", value)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", snake_case_str).lower()


def common_rename_to_standard_columns_name(df_columns: list) -> list:
    """
    use for rename list of string to follow snake name rule.

    @param df_columns: list of columns name
    @return: list of renamed columns
    """
    # " ".join(s.split()) to remove extra space
    return [common_string_to_snake_case("_".join(x.split())) for x in df_columns]


def check_if_all_same_dict(dicts_list: list) -> bool:
    """
    Compare list of dicts that if its have the same key and value or not.

    @param dicts_list: List of dict need to compare
    @return: bool for is valid or not
    """
    is_valid_dict = True
    dicts_list_len = len(dicts_list)
    if dicts_list:
        if dicts_list_len == 1:
            return is_valid_dict
        for i in range(dicts_list_len - 1):
            check_result = all(
                (dicts_list[i + 1].get(k) == v for k, v in dicts_list[i].items())
            )
            if not check_result:
                is_valid_dict = False
                break
        return is_valid_dict
    return False
