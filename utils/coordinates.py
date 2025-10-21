"""
반출한 GRID데이터를 5179좌표계로 변환하는 프로세스

Input: 원본 parquet
Output: 그리드컬럼을 제외한 5179
"""

import pandas as pd
from shapely.geometry import Point
import pyarrow

X_MAPPING = list("ABCDEFG")
Y_MAPPING = list("BCDEFGH")

def _convert_single_grid_to_point(grid_id: str) -> Point | None:
    if not isinstance(grid_id, str) or len(grid_id) < 12:
        return None

    try:
        x_char, y_char = grid_id[0], grid_id[1]

        x_index = X_MAPPING.index(x_char)
        y_index = Y_MAPPING.index(y_char)

        x_tail = int(grid_id[2:7])
        y_tail = int(grid_id[7:])

    except (ValueError, IndexError):
        return None

    x_coord = (x_index + 7) * 100_000 + x_tail
    y_coord = (y_index + 14) * 100_000 + y_tail

    return Point(x_coord, y_coord)

def grid_to_5179_wkt(grid_ids:pd.Series) -> pd.Series:
    """
    DuckDB가 지리공간 타입을 직접 지원하지 않음으로
    그리드 ID 시리즈 -> EPSG:5179의 WKT 문자열 시리즈로 변환함.
    """
    if isinstance(grid_ids, (pyarrow.ChunkedArray, pyarrow.Array)):
        # to_pandas() 메서드를 사용하여 Series로 변환
        grid_ids = grid_ids.to_pandas()

    points = grid_ids.apply(_convert_single_grid_to_point)

    return points.apply(lambda p: p.wkt if p is not None else None)

