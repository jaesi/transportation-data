import os
import duckdb
import numpy as np

def ensure_dirs():
    "필요한 디렉토리 생성"
    from config import CONFIG
    os.makedirs(CONFIG['CHECKPOINT_DIR'], exist_ok=True)
    os.makedirs(CONFIG['OUTPUT_DIR'], exist_ok=True)

def db_connection(read_only=False):
    # DuckDB 연결
    from config import CONFIG
    con = duckdb.connect(CONFIG['DB_PATH'], read_only=read_only)
    con.execute("SET memory_limit='100gb'")
    return con

def between_months(s, e):
    months = []
    year, month =divmod(s, 100)
    while True:
        months.append(f"{year}{month:02d}")
        if f"{year}{month:02d}" == str(e):
            break
        month += 1
        if month > 12:
            year += 1
            month = 1
    return months

# x, y 좌표 -> grid로 변환
def to_grid_vec(x, y):
    xi = np.floor(x / 100000 -7).astype('int64')
    yi = np.floor(y / 100000 -14).astype('int64')

    # 알파벳 매칭 
    ox_mapping = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
    oy_mapping = ['B', 'C', 'D', 'E', 'F', 'G', 'H']
    max_idx = len(ox_mapping) - 1

    xi_clipped = np.clip(xi, 0, max_idx)
    yi_clipped = np.clip(yi, 0, max_idx)

    # 접두사 생성
    prefix = np.char.add(
        np.array(ox_mapping)[xi_clipped],
        np.array(oy_mapping)[yi_clipped]
    )
    # 접미사 생성
    mod = 100000
    x_suffix = (np.floor(np.abs(x)).astype('int64') % mod)
    x_suffix_str = np.char.zfill(x_suffix.astype(str), 5)

    y_suffix = (np.floor(np.abs(y)).astype('int64') % mod)
    y_suffix_str = np.char.zfill(y_suffix.astype(str), 5)

    suffix = np.char.add(x_suffix_str, y_suffix_str)

    grid_id = np.char.add(prefix, suffix)

    return grid_id