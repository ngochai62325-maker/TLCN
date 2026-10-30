import html
import os
from collections import defaultdict

# Đổi thành True nếu muốn gom 15 cột kỹ thuật (_ingestion_*, silver_*, ...) thành 1 dòng cho sơ đồ ngắn gọn hơn
COMPACT_LINEAGE = False
BASE_OUTPUT_NAME = "rice_lakehouse_gold"


def get_new_output_path(base_name, ext=".drawio"):
    """Luôn trả về một tên file mới chưa từng tồn tại để không ghi đè file cũ."""
    counter = 1
    while True:
        candidate = f"{base_name}_v{counter}{ext}"
        if not os.path.exists(candidate):
            return candidate
        counter += 1


LINEAGE_COLS = [
    ("_ingestion_run_id", "varchar", "NOT NULL"),
    ("_ingestion_batch_id", "varchar", "NOT NULL"),
    ("_ingestion_chunk_id", "bigint", "NOT NULL"),
    ("_ingestion_timestamp", "timestamptz", "NOT NULL"),
    ("_source_id", "varchar", "NOT NULL"),
    ("_source_file", "varchar", "NOT NULL"),
    ("_source_checksum", "varchar", "NOT NULL"),
    ("_source_snapshot_id", "bigint", "NOT NULL"),
    ("_silver_processed_at", "timestamptz", "NOT NULL"),
    ("dq_warnings_json", "varchar", "NOT NULL"),
    ("silver_table_uuid", "varchar", "NOT NULL"),
    ("silver_read_snapshot_id", "bigint", "NOT NULL"),
    ("source_business_key_json", "varchar", "NOT NULL"),
    ("source_context_json", "varchar", "NOT NULL"),
    ("gold_model_version", "varchar", "NOT NULL"),
]


def fact_tail():
    base = [
        ("source_system_key", "varchar(64)", "FK, NOT NULL"),
        ("missing_reason", "varchar", ""),
    ]
    if COMPACT_LINEAGE:
        return base + [
            ("_lineage_metadata_15_cols", "bundle", "NOT NULL")
        ]
    return base + LINEAGE_COLS


# Bố cục 3 dải cột dọc với 2 hành lang đi dây riêng biệt ở giữa (X=500..860 và X=1320..1680)
GROUPS = {
    "G1": {
        "title": "01 - PRODUCTION",
        "fill": "#EAF3FF",
        "stroke": "#9BBCE0",
        "header_fill": "#D4E6FF",
        "x": 60,
        "y": 40,
        "table_gap_y": 35,
    },
    "G3": {
        "title": "03 - TRADE",
        "fill": "#EAF7EC",
        "stroke": "#98C9A3",
        "header_fill": "#D1E7DD",
        "x": 60,
        "y": 800,
        "table_gap_y": 35,
    },
    "G4": {
        "title": "04 - SUPPLY & UTILIZATION",
        "fill": "#F2ECFA",
        "stroke": "#B59FD1",
        "header_fill": "#E2D9F3",
        "x": 60,
        "y": 1590,
        "table_gap_y": 35,
    },
    "G5": {
        "title": "05 - CROSS-DOMAIN / SOURCE-SPECIFIC",
        "fill": "#FCECEF",
        "stroke": "#DDA0A7",
        "header_fill": "#F8D7DA",
        "x": 60,
        "y": 2350,
        "table_gap_y": 45,
    },
    "G6": {
        "title": "06 - SHARED DIMENSIONS",
        "fill": "#F1F3F5",
        "stroke": "#ADB5BD",
        "header_fill": "#DEE2E6",
        "x": 860,
        "y": 40,
        "table_gap_y": 140,
    },
    "G2": {
        "title": "02 - PRICE",
        "fill": "#FFF4DF",
        "stroke": "#E0C27B",
        "header_fill": "#FFE69C",
        "x": 1680,
        "y": 40,
        "table_gap_y": 75,
    },
}

# --- Bảng màu mới theo tư vấn ---
# Header của bảng FACT đổi theo tông TableGroup chứa nó
FACT_HEADER_COLORS = {
    "G1": "#2B6CB0",   # Production  – xanh biển
    "G3": "#2F855A",   # Trade       – xanh lục
    "G4": "#6B46C1",   # Supply      – tím
    "G5": "#C53030",   # Cross-domain– đỏ đô
    "G2": "#B7791F",   # Price       – hổ phách
}
# Bảng Dimension giữ màu xanh slate đậm riêng để phân biệt Fact/Dim
DIM_HEADER_COLOR = "#1E3A5F"

# Bảng màu dây theo Dimension đích (semantic edge coloring)
EDGE_COLOR_TIME = "#2563EB"          # dim_year / dim_month / dim_date
EDGE_COLOR_GEO = "#059669"           # dim_source_geography
EDGE_COLOR_PRODUCT = "#7C3AED"       # dim_source_product
EDGE_COLOR_OBS_TYPE = "#EA580C"      # dim_observation_type
EDGE_COLOR_PRICE_SERIES = "#D97706"  # dim_price_series
EDGE_COLOR_SYSTEM = "#94A3B8"        # dim_source_system (nét đứt, kỹ thuật/lineage)
EDGE_COLOR_FALLBACK = "#7A869A"


def edge_style_for(dst_tbl):
    """Trả về (màu, dashed_bool) theo bảng Dimension đích."""
    if dst_tbl == "dim_source_system":
        return EDGE_COLOR_SYSTEM, True
    if dst_tbl in ("dim_year", "dim_month", "dim_date"):
        return EDGE_COLOR_TIME, False
    if dst_tbl == "dim_source_geography":
        return EDGE_COLOR_GEO, False
    if dst_tbl == "dim_source_product":
        return EDGE_COLOR_PRODUCT, False
    if dst_tbl == "dim_observation_type":
        return EDGE_COLOR_OBS_TYPE, False
    if dst_tbl == "dim_price_series":
        return EDGE_COLOR_PRICE_SERIES, False
    return EDGE_COLOR_FALLBACK, False


TABLES = {
    # --- CỘT TRÁI: CÁC NHÓM FACT SẢN LƯỢNG, THƯƠNG MẠI, CUNG CẦU, ĐA MIỀN ---
    "fact_fao_production_observation": {
        "group": "G1",
        "columns": [
            ("observation_key", "varchar(64)", "PK"),
            ("year_key", "integer", "FK, UQ, NOT NULL"),
            ("geography_key", "varchar(64)", "FK, UQ, NOT NULL"),
            ("product_key", "varchar(64)", "FK, UQ, NOT NULL"),
            ("observation_type_key", "varchar(64)", "FK, UQ, NOT NULL"),
            ("value", "decimal(18,2)", ""),
        ] + fact_tail(),
    },
    "fact_fao_trade_observation": {
        "group": "G3",
        "columns": [
            ("observation_key", "varchar(64)", "PK"),
            ("year_key", "integer", "FK, UQ, NOT NULL"),
            ("reporter_geography_key", "varchar(64)", "FK, UQ, NOT NULL"),
            ("partner_geography_key", "varchar(64)", "FK, UQ, NOT NULL"),
            ("product_key", "varchar(64)", "FK, UQ, NOT NULL"),
            ("observation_type_key", "varchar(64)", "FK, UQ, NOT NULL"),
            ("value", "decimal(22,2)", ""),
        ] + fact_tail(),
    },
    "fact_fao_sua_observation": {
        "group": "G4",
        "columns": [
            ("observation_key", "varchar(64)", "PK"),
            ("year_key", "integer", "FK, UQ, NOT NULL"),
            ("geography_key", "varchar(64)", "FK, UQ, NOT NULL"),
            ("product_key", "varchar(64)", "FK, UQ, NOT NULL"),
            ("observation_type_key", "varchar(64)", "FK, UQ, NOT NULL"),
            ("value", "decimal(18,2)", ""),
        ] + fact_tail(),
    },
    "fact_usda_psd_observation": {
        "group": "G5",
        "columns": [
            ("observation_key", "varchar(64)", "PK"),
            ("geography_key", "varchar(64)", "FK, UQ, NOT NULL"),
            ("product_key", "varchar(64)", "FK, UQ, NOT NULL"),
            ("observation_type_key", "varchar(64)", "FK, UQ, NOT NULL"),
            ("market_year", "integer", "UQ, NOT NULL"),
            ("crop_year", "varchar", "NOT NULL"),
            ("value", "decimal(22,4)", ""),
        ] + fact_tail(),
    },
    "fact_nso_observation": {
        "group": "G5",
        "columns": [
            ("observation_key", "varchar(64)", "PK"),
            ("year_key", "integer", "FK, UQ, NOT NULL"),
            ("geography_key", "varchar(64)", "FK, NOT NULL"),
            ("source_table", "varchar", "UQ, NOT NULL"),
            ("source_geography_key", "varchar", "UQ, NOT NULL"),
            ("source_column", "varchar", "NOT NULL"),
            ("measure", "varchar", "UQ, NOT NULL"),
            ("season", "varchar", "UQ, NOT NULL"),
            ("statistic_kind", "varchar", "UQ, NOT NULL"),
            ("period_raw", "varchar", "NOT NULL"),
            ("release_status", "varchar", "NOT NULL"),
            ("quality_status", "varchar", "NOT NULL"),
            ("review_reason", "varchar", ""),
            ("value", "decimal(28,8)", ""),
            ("unit", "varchar", "NOT NULL"),
            ("value_source", "decimal(28,8)", ""),
            ("silver_conversion_factor", "decimal(8,0)", "NOT NULL"),
            ("bronze_read_snapshot_id", "bigint", "NOT NULL"),
            ("diagnostic_evidence_json", "varchar", "NOT NULL"),
        ] + fact_tail(),
    },

    # --- CỘT GIỮA: 8 BẢNG SHARED DIMENSIONS XẾP DỌC 1 CỘT ---
    "dim_year": {
        "group": "G6",
        "columns": [
            ("year_key", "integer", "PK"),
            ("decade", "integer", "NOT NULL"),
            ("is_leap_year", "boolean", "NOT NULL"),
        ],
    },
    "dim_month": {
        "group": "G6",
        "columns": [
            ("month_key", "integer", "PK"),
            ("year_key", "integer", "FK, NOT NULL"),
            ("month_number", "integer", "NOT NULL"),
            ("quarter_number", "integer", "NOT NULL"),
            ("month_start", "date", "NOT NULL"),
        ],
    },
    "dim_date": {
        "group": "G6",
        "columns": [
            ("date_key", "integer", "PK"),
            ("full_date", "date", "UQ, NOT NULL"),
            ("month_key", "integer", "FK, NOT NULL"),
            ("iso_weekday", "integer", "NOT NULL"),
            ("day_of_month", "integer", "NOT NULL"),
        ],
    },
    "dim_source_geography": {
        "group": "G6",
        "columns": [
            ("geography_key", "varchar(64)", "PK"),
            ("identity_namespace", "varchar", "UQ, NOT NULL"),
            ("identity_scope", "varchar", "UQ, NOT NULL"),
            ("source_identity", "varchar", "UQ, NOT NULL"),
            ("source_labels_json", "varchar", "NOT NULL"),
            ("source_level", "varchar", "NOT NULL"),
            ("interpretation_status", "varchar", "NOT NULL"),
        ],
    },
    "dim_source_product": {
        "group": "G6",
        "columns": [
            ("product_key", "varchar(64)", "PK"),
            ("identity_namespace", "varchar", "UQ, NOT NULL"),
            ("source_identity", "varchar", "UQ, NOT NULL"),
            ("source_labels_json", "varchar", "NOT NULL"),
            ("identity_kind", "varchar", "NOT NULL"),
        ],
    },
    "dim_observation_type": {
        "group": "G6",
        "columns": [
            ("observation_type_key", "varchar(64)", "PK"),
            ("source_system_key", "varchar(64)", "FK, UQ, NOT NULL"),
            ("source_metric_id", "varchar", "UQ, NOT NULL"),
            ("source_labels_json", "varchar", "NOT NULL"),
            ("silver_unit", "varchar", "NOT NULL"),
            ("gold_unit", "varchar", "NOT NULL"),
            ("gold_conversion_factor", "decimal(10,4)", "NOT NULL"),
            ("aggregation_rule", "varchar", "NOT NULL"),
        ],
    },
    "dim_price_series": {
        "group": "G6",
        "columns": [
            ("price_series_key", "varchar(64)", "PK"),
            ("source_system_key", "varchar(64)", "FK, UQ, NOT NULL"),
            ("series_identity_json", "varchar", "UQ, NOT NULL"),
            ("geography_key", "varchar(64)", "FK, NOT NULL"),
            ("product_key", "varchar(64)", "FK, NOT NULL"),
            ("price_type_source", "varchar", ""),
            ("unit", "varchar", "NOT NULL"),
            ("currency", "varchar", ""),
            ("price_kind", "varchar", "NOT NULL"),
            ("information_source", "varchar", ""),
        ],
    },
    "dim_source_system": {
        "group": "G6",
        "columns": [
            ("source_system_key", "varchar(64)", "PK"),
            ("source_id", "varchar", "UQ, NOT NULL"),
            ("provider", "varchar", "NOT NULL"),
            ("dataset_label", "varchar", "NOT NULL"),
        ],
    },

    # --- CỘT PHẢI: 4 BẢNG GIÁ (02 - PRICE) XẾP DỌC 1 CỘT ---
    "fact_fao_price_annual": {
        "group": "G2",
        "columns": [
            ("observation_key", "varchar(64)", "PK"),
            ("year_key", "integer", "FK, UQ, NOT NULL"),
            ("price_series_key", "varchar(64)", "FK, UQ, NOT NULL"),
            ("value", "decimal(18,4)", ""),
        ] + fact_tail(),
    },
    "fact_price_monthly": {
        "group": "G2",
        "columns": [
            ("observation_key", "varchar(64)", "PK"),
            ("month_key", "integer", "FK, UQ, NOT NULL"),
            ("price_series_key", "varchar(64)", "FK, UQ, NOT NULL"),
            ("value", "decimal(20,4)", ""),
        ] + fact_tail(),
    },
    "fact_vn_market_quote_daily": {
        "group": "G2",
        "columns": [
            ("observation_key", "varchar(64)", "PK"),
            ("date_key", "integer", "FK, UQ, NOT NULL"),
            ("price_series_key", "varchar(64)", "FK, UQ, NOT NULL"),
            ("value", "decimal(18,2)", ""),
        ] + fact_tail(),
    },
    "fact_usda_price_reference": {
        "group": "G2",
        "columns": [
            ("observation_key", "varchar(64)", "PK"),
            ("price_series_key", "varchar(64)", "FK, UQ, NOT NULL"),
            ("reference_year", "integer", "UQ, NOT NULL"),
            ("reference_period", "varchar", "UQ, NOT NULL"),
            ("value", "decimal(18,2)", ""),
        ] + fact_tail(),
    },
}

FOREIGN_KEYS = [
    # Fact Cột Trái -> Shared Dimensions
    ("fact_fao_production_observation", "year_key", "dim_year", "year_key"),
    ("fact_fao_production_observation", "geography_key", "dim_source_geography", "geography_key"),
    ("fact_fao_production_observation", "product_key", "dim_source_product", "product_key"),
    ("fact_fao_production_observation", "observation_type_key", "dim_observation_type", "observation_type_key"),
    ("fact_fao_production_observation", "source_system_key", "dim_source_system", "source_system_key"),

    ("fact_fao_trade_observation", "year_key", "dim_year", "year_key"),
    ("fact_fao_trade_observation", "reporter_geography_key", "dim_source_geography", "geography_key"),
    ("fact_fao_trade_observation", "partner_geography_key", "dim_source_geography", "geography_key"),
    ("fact_fao_trade_observation", "product_key", "dim_source_product", "product_key"),
    ("fact_fao_trade_observation", "observation_type_key", "dim_observation_type", "observation_type_key"),
    ("fact_fao_trade_observation", "source_system_key", "dim_source_system", "source_system_key"),

    ("fact_fao_sua_observation", "year_key", "dim_year", "year_key"),
    ("fact_fao_sua_observation", "geography_key", "dim_source_geography", "geography_key"),
    ("fact_fao_sua_observation", "product_key", "dim_source_product", "product_key"),
    ("fact_fao_sua_observation", "observation_type_key", "dim_observation_type", "observation_type_key"),
    ("fact_fao_sua_observation", "source_system_key", "dim_source_system", "source_system_key"),

    ("fact_usda_psd_observation", "geography_key", "dim_source_geography", "geography_key"),
    ("fact_usda_psd_observation", "product_key", "dim_source_product", "product_key"),
    ("fact_usda_psd_observation", "observation_type_key", "dim_observation_type", "observation_type_key"),
    ("fact_usda_psd_observation", "source_system_key", "dim_source_system", "source_system_key"),

    ("fact_nso_observation", "year_key", "dim_year", "year_key"),
    ("fact_nso_observation", "geography_key", "dim_source_geography", "geography_key"),
    ("fact_nso_observation", "source_system_key", "dim_source_system", "source_system_key"),

    # Dim -> Dim (Nội bộ Shared Dimensions)
    ("dim_month", "year_key", "dim_year", "year_key"),
    ("dim_date", "month_key", "dim_month", "month_key"),
    ("dim_observation_type", "source_system_key", "dim_source_system", "source_system_key"),
    ("dim_price_series", "source_system_key", "dim_source_system", "source_system_key"),
    ("dim_price_series", "geography_key", "dim_source_geography", "geography_key"),
    ("dim_price_series", "product_key", "dim_source_product", "product_key"),

    # Fact Cột Phải (Price) -> Shared Dimensions
    ("fact_fao_price_annual", "year_key", "dim_year", "year_key"),
    ("fact_fao_price_annual", "price_series_key", "dim_price_series", "price_series_key"),
    ("fact_fao_price_annual", "source_system_key", "dim_source_system", "source_system_key"),

    ("fact_price_monthly", "month_key", "dim_month", "month_key"),
    ("fact_price_monthly", "price_series_key", "dim_price_series", "price_series_key"),
    ("fact_price_monthly", "source_system_key", "dim_source_system", "source_system_key"),

    ("fact_vn_market_quote_daily", "date_key", "dim_date", "date_key"),
    ("fact_vn_market_quote_daily", "price_series_key", "dim_price_series", "price_series_key"),
    ("fact_vn_market_quote_daily", "source_system_key", "dim_source_system", "source_system_key"),

    ("fact_usda_price_reference", "price_series_key", "dim_price_series", "price_series_key"),
    ("fact_usda_price_reference", "source_system_key", "dim_source_system", "source_system_key"),
]


def format_dbdiagram_row(cname, ctype, cmeta, table_w):
    """Tạo giao diện dòng 2 cột Trái - Phải chuẩn dbdiagram.io"""
    is_pk = "PK" in cmeta
    is_fk = "FK" in cmeta
    is_nn = "NOT NULL" in cmeta
    is_uq = "UQ" in cmeta

    if is_pk:
        left_html = (
            f'<b style="color:#1F2328;">{html.escape(cname)}</b>'
            f'<span style="filter:grayscale(100%) opacity(65%); font-size:11px; margin-left:5px;">&#128273;</span>'
        )
    elif is_fk:
        left_html = (
            f'<span style="color:#24292F;">{html.escape(cname)}</span>'
            f'<span style="filter:grayscale(100%) opacity(50%); font-size:10px; margin-left:4px;">&#128279;</span>'
        )
    else:
        left_html = f'<span style="color:#333B42;">{html.escape(cname)}</span>'

    badges = ""
    if is_uq:
        badges += (
            '<span style="background-color:#E8F0FE; color:#3B6E9C; border-radius:3px; '
            'padding:1px 4px; font-size:9px; font-weight:600; margin-left:5px;">UQ</span>'
        )
    if is_nn:
        badges += (
            '<span style="background-color:#ECEEF0; color:#768390; border-radius:3px; '
            'padding:1px 4px; font-size:9px; font-weight:600; margin-left:5px;">NN</span>'
        )

    right_html = f'<span style="color:#6E7781; font-family:Consolas,monospace,sans-serif;">{html.escape(ctype)}</span>{badges}'

    inner_w = table_w - 4
    return (
        f'<table cellpadding="0" cellspacing="0" style="width:{inner_w}px; font-family:-apple-system,BlinkMacSystemFont,\'Segoe UI\',Roboto,sans-serif; font-size:12px; border-collapse:collapse;">'
        f'<tr>'
        f'<td align="left" style="padding-left:12px; white-space:nowrap;">{left_html}</td>'
        f'<td align="right" style="padding-right:12px; white-space:nowrap;">{right_html}</td>'
        f'</tr>'
        f'</table>'
    )


def format_dbdiagram_header(tname):
    """Thanh tiêu đề bảng chuẩn dbdiagram, bỏ tiền tố gold_local."""
    return (
        f'<span style="font-family:-apple-system,BlinkMacSystemFont,\'Segoe UI\',Roboto,sans-serif; font-size:13px; color:#FFFFFF;">'
        f'&#9635;&nbsp; <b>{html.escape(tname)}</b>'
        f'</span>'
    )


def generate_drawio():
    cells = [
        '<mxCell id="0"/>',
        '<mxCell id="1" parent="0"/>',
    ]

    TABLE_W = 370
    HEADER_H = 34
    ROW_H = 26
    PAD_X = 25
    PAD_Y = 48

    group_tables = {gid: [] for gid in GROUPS}
    for tname, tinfo in TABLES.items():
        group_tables[tinfo["group"]].append(tname)

    row_global_box = {}

    left_group_order = ["G1", "G3", "G4", "G5"]
    current_left_y = 40

    for gid, ginfo in GROUPS.items():
        tnames = group_tables[gid]
        gap_y = ginfo["table_gap_y"]
        cur_y = PAD_Y
        table_coords = {}

        for tname in tnames:
            n_rows = len(TABLES[tname]["columns"])
            th = HEADER_H + n_rows * ROW_H
            tx = PAD_X
            ty = cur_y
            table_coords[tname] = (tx, ty, TABLE_W, th)
            cur_y += th + gap_y

        gw = PAD_X * 2 + TABLE_W
        gh = cur_y - gap_y + PAD_X

        if gid in left_group_order:
            ginfo["y"] = current_left_y
            current_left_y += gh + 40

        gx, gy = ginfo["x"], ginfo["y"]

        g_style = (
            f"swimlane;startSize=34;fillColor={ginfo['header_fill']};"
            f"swimlaneFillColor={ginfo['fill']};strokeColor={ginfo['stroke']};"
            f"fontStyle=1;fontSize=13;fontColor=#24292F;rounded=1;arcSize=2;collapsible=0;whiteSpace=wrap;html=1;"
        )
        cells.append(
            f'<mxCell id="{gid}" value="{html.escape(ginfo["title"])}" style="{g_style}" vertex="1" parent="1">'
            f'<mxGeometry x="{gx}" y="{gy}" width="{gw}" height="{gh}" as="geometry"/>'
            f"</mxCell>"
        )

        for tname in tnames:
            tx, ty, tw, th = table_coords[tname]
            t_id = f"tbl_{tname}"
            header_html = format_dbdiagram_header(tname)

            # --- Màu header: Dimension vs Fact ---
            grp = TABLES[tname]["group"]
            if grp == "G6":
                header_color = DIM_HEADER_COLOR
            else:
                header_color = FACT_HEADER_COLORS.get(grp, "#2E6593")

            t_style = (
                f"swimlane;fontStyle=0;childLayout=stackLayout;horizontal=1;startSize={HEADER_H};"
                f"fillColor={header_color};swimlaneFillColor=#FFFFFF;strokeColor=#CBD5E1;"
                f"horizontalStack=0;resizeParent=1;resizeParentMax=0;resizeLast=0;collapsible=0;"
                f"marginBottom=0;rounded=1;arcSize=4;shadow=0;whiteSpace=wrap;html=1;"
            )
            cells.append(
                f'<mxCell id="{t_id}" value="{html.escape(header_html)}" style="{t_style}" vertex="1" parent="{gid}">'
                f'<mxGeometry x="{tx}" y="{ty}" width="{tw}" height="{th}" as="geometry"/>'
                f"</mxCell>"
            )

            for r_idx, (cname, ctype, cmeta) in enumerate(TABLES[tname]["columns"]):
                c_id = f"col_{tname}_{cname}"
                ry = HEADER_H + r_idx * ROW_H
                row_bg = "#F8FAFC" if "PK" in cmeta else "#FFFFFF"
                row_html = format_dbdiagram_row(cname, ctype, cmeta, tw)

                c_style = (
                    f"text;strokeColor=#EEF1F4;fillColor={row_bg};align=left;verticalAlign=middle;"
                    f"spacingLeft=0;spacingRight=0;overflow=hidden;rotatable=0;"
                    f"points=[[0,0.5],[1,0.5]];portConstraint=eastwest;whiteSpace=wrap;html=1;"
                )
                cells.append(
                    f'<mxCell id="{c_id}" value="{html.escape(row_html)}" style="{c_style}" vertex="1" parent="{t_id}">'
                    f'<mxGeometry y="{ry}" width="{tw}" height="{ROW_H}" as="geometry"/>'
                    f"</mxCell>"
                )

                row_global_box[(tname, cname)] = (
                    gx + tx,
                    gx + tx + tw,
                    gy + ty + ry,
                    ROW_H,
                )

    left_corridor_edges = []
    dim_internal_edges = []
    right_corridor_edges = []

    for edge in FOREIGN_KEYS:
        src_tbl, src_col, dst_tbl, dst_col = edge
        src_grp = TABLES[src_tbl]["group"]
        if src_grp in ("G1", "G3", "G4", "G5"):
            left_corridor_edges.append(edge)
        elif src_grp == "G6":
            dim_internal_edges.append(edge)
        else:
            right_corridor_edges.append(edge)

    port_incoming = defaultdict(list)
    for edge in left_corridor_edges:
        port_incoming[(edge[2], edge[3], "L")].append(edge)
    for edge in dim_internal_edges + right_corridor_edges:
        port_incoming[(edge[2], edge[3], "R")].append(edge)

    edge_entry_y_frac = {}
    for port_key, edges in port_incoming.items():
        edges_sorted = sorted(edges, key=lambda e: row_global_box[(e[0], e[1])][2])
        n = len(edges_sorted)
        for idx, e in enumerate(edges_sorted):
            if n == 1:
                edge_entry_y_frac[(e, port_key[2])] = 0.5
            else:
                edge_entry_y_frac[(e, port_key[2])] = round(0.16 + (0.68 * idx / (n - 1)), 3)

    all_routed_edges = []

    # --- Sắp làn dây theo thứ tự dòng NGUỒN từ trên xuống, rồi mới tới dòng ĐÍCH ---
    left_sorted = sorted(
        left_corridor_edges,
        key=lambda e: (row_global_box[(e[0], e[1])][2], row_global_box[(e[2], e[3])][2]),
    )
    for idx, e in enumerate(left_sorted):
        lane_x = 505 + idx * 15  # nới nhẹ 14 -> 15
        all_routed_edges.append((e, 1, 0, lane_x, edge_entry_y_frac[(e, "L")]))

    dim_sorted = sorted(
        dim_internal_edges,
        key=lambda e: (row_global_box[(e[0], e[1])][2], row_global_box[(e[2], e[3])][2]),
    )
    for idx, e in enumerate(dim_sorted):
        lane_x = 1305 + idx * 16  # nới nhẹ 15 -> 16
        all_routed_edges.append((e, 1, 1, lane_x, edge_entry_y_frac[(e, "R")]))

    right_sorted = sorted(
        right_corridor_edges,
        key=lambda e: (row_global_box[(e[0], e[1])][2], row_global_box[(e[2], e[3])][2]),
    )
    for idx, e in enumerate(right_sorted):
        lane_x = 1425 + idx * 21  # nới nhẹ 20 -> 21
        all_routed_edges.append((e, 0, 1, lane_x, edge_entry_y_frac[(e, "R")]))

    for e_idx, (edge, exit_x, entry_x, lane_x, dst_y_frac) in enumerate(all_routed_edges, start=1):
        src_tbl, src_col, dst_tbl, dst_col = edge
        src_id = f"col_{src_tbl}_{src_col}"
        dst_id = f"col_{dst_tbl}_{dst_col}"

        _, _, src_top_y, src_h = row_global_box[(src_tbl, src_col)]
        _, _, dst_top_y, dst_h = row_global_box[(dst_tbl, dst_col)]

        src_y = round(src_top_y + 0.5 * src_h, 1)
        dst_y = round(dst_top_y + dst_y_frac * dst_h, 1)

        # --- Semantic edge coloring ---
        edge_color, is_dashed = edge_style_for(dst_tbl)
        if is_dashed:
            stroke_w = 1.0
            dash_part = "dashed=1;dashPattern=6 4;"
        else:
            stroke_w = 1.2
            dash_part = ""

        e_style = (
            f"edgeStyle=orthogonalEdgeStyle;rounded=1;arcSize=8;orthogonalLoop=1;jettySize=auto;html=1;"
            f"exitX={exit_x};exitY=0.5;exitDx=0;exitDy=0;"
            f"entryX={entry_x};entryY={dst_y_frac};entryDx=0;entryDy=0;"
            f"startArrow=ERmany;startFill=0;endArrow=ERone;endFill=0;"
            f"jumpStyle=arc;jumpSize=6;strokeColor={edge_color};strokeWidth={stroke_w};{dash_part}"
        )
        cells.append(
            f'<mxCell id="fk_edge_{e_idx}" value="" style="{e_style}" edge="1" parent="1" source="{src_id}" target="{dst_id}">'
            f'<mxGeometry relative="1" as="geometry">'
            f'<Array as="points">'
            f'<mxPoint x="{lane_x}" y="{src_y}"/>'
            f'<mxPoint x="{lane_x}" y="{dst_y}"/>'
            f'</Array>'
            f'</mxGeometry>'
            f"</mxCell>"
        )

    xml_content = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<mxfile host="app.diagrams.net" type="device">\n'
        '  <diagram id="rice_lakehouse_gold_20261025" name="gold-20261025.1">\n'
        '    <mxGraphModel dx="2400" dy="2000" grid="1" gridSize="10" guides="1" tooltips="1" connect="1" arrows="1" fold="1" page="0" math="0" shadow="0">\n'
        "      <root>\n        "
        + "\n        ".join(cells)
        + "\n      </root>\n"
        "    </mxGraphModel>\n"
        "  </diagram>\n"
        "</mxfile>\n"
    )

    # Luôn tạo file mới độc lập bằng mode="x" (exclusive creation)
    output_file = get_new_output_path(BASE_OUTPUT_NAME)
    with open(output_file, "x", encoding="utf-8") as f:
        f.write(xml_content)
    print(f"Đã tạo file mới thành công: {output_file}")


if __name__ == "__main__":
    generate_drawio()