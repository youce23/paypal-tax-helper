import os

import pandas as pd


# === TTMデータの読み込み・前処理 ===
def load_ttm(ttm_file: str) -> pd.DataFrame:
    """TTM（為替レート）CSVを読み込み、日付・欠損値を整形"""
    df = pd.read_csv(ttm_file)
    df = df[df["日付"].apply(is_valid_date)].copy()
    df["日付"] = pd.to_datetime(df["日付"])
    df["TTM"] = pd.to_numeric(df["TTM"], errors="coerce")
    df = df.sort_values("日付")
    df["TTM"] = df["TTM"].ffill()
    return df.set_index("日付")


def is_valid_date(date_str: str) -> bool:
    """日付形式であるか確認するヘルパー関数"""
    try:
        pd.to_datetime(date_str)
        return True
    except Exception:
        return False


# === 取引データの読み込み ===
def load_transactions(file_path: str) -> pd.DataFrame:
    """取引CSVを読み込み、日付・金額形式を整形"""
    df = pd.read_csv(file_path)
    df["日付"] = pd.to_datetime(df["日付"])
    df["正味"] = df["正味"].astype(str).str.replace(",", "").astype(float)
    return df


# === 入金処理 ===
def process_income(row, ttm_df: pd.DataFrame, usd_balance: float, accumulated_jpy_income: float, income_records: list):
    """
    入金トランザクションを処理し、必要なデータを記録する関数。

    Args:
        row (pd.Series): 取引データの1行（入金トランザクションの詳細）
        ttm_df (pd.DataFrame): TTM（為替レート）データ
        usd_balance (float): 現在のUSD残高
        accumulated_jpy_income (float): 現在までに累積されたJPY換算額（雑所得）
        income_records (list): 入金明細を格納するリスト

    Returns:
        tuple: 更新されたUSD残高、累積JPY所得
    """

    date = row["日付"].date()  # トランザクションの日付を抽出
    amount = row["正味"]  # 入金額（USD）を抽出
    ttm = ttm_df.loc[pd.Timestamp(date)]["TTM"]  # TTM（為替レート）を取得

    jpy_income = amount * ttm  # USD入金額をJPY換算した雑所得額を計算

    usd_balance += amount  # 残高を更新（USD入金額を加算）
    accumulated_jpy_income += jpy_income  # 累積JPY換算額を更新（雑所得の追加）

    # 入金データをリストに追加
    income_records.append(
        {
            "入金日": date,  # 入金日
            "USD入金額": amount,  # 入金額（USD）
            "入金時TTM": ttm,  # 入金時のTTM
            "JPY換算額（雑所得）": jpy_income,  # 入金額をJPY換算した雑所得額
        }
    )

    # 更新したUSD残高と累積JPY所得を返す
    return usd_balance, accumulated_jpy_income


# === 出金処理 ===
def process_withdrawal(
    row,
    ttm_df: pd.DataFrame,
    usd_balance: float,
    accumulated_jpy_income: float,
    withdrawal_records: list,
    df: pd.DataFrame,
) -> tuple[float, float, list]:
    """出金トランザクションを処理し、必要なデータを記録

    引落し（出金）の際、USD残高をJYP換算し、その結果をレポートに記録します。
    さらに、出金時の為替損益とスプレッド（経費）を計算し、必要なデータを更新します。

    Args:
        row (pd.Series): 出金トランザクションの1行（取引情報）
        ttm_df (pd.DataFrame): TTM（為替レート）データフレーム
        usd_balance (float): 現在のUSD残高
        accumulated_jpy_income (float): 累積されたJPY雑所得額（過去の入金分）
        withdrawal_records (list): 出金レコードのリスト（出金明細を格納）
        df (pd.DataFrame): 取引全体のデータフレーム（出金に関連するJPYのデータを取得するため）

    Returns:
        tuple: 更新されたUSD残高、累積JPY雑所得額、更新された出金レコードのリスト
    """
    date = row["日付"].date()  # 出金日を取得

    if usd_balance == 0:  # 残高が0ならスキップ
        return usd_balance, accumulated_jpy_income, withdrawal_records

    ttm_out = ttm_df.loc[pd.Timestamp(date)]["TTM"]  # 出金日のTTMを取得
    jpy_evaluated = usd_balance * ttm_out  # USD残高をJPYに換算

    # 出金日・通貨・残高への影響、を条件に、対応するJPYの引き落としを検索
    jpy_out_row = df[(df["日付"].dt.date == date) & (df["通貨"] == "JPY") & (df["残高への影響"] == "引落し")]
    if jpy_out_row.empty:  # JPY出金がなければスキップ
        return usd_balance, accumulated_jpy_income, withdrawal_records

    jpy_out = -jpy_out_row.iloc[0]["正味"]  # JPY出金額を取得
    fx_profit = jpy_evaluated - accumulated_jpy_income  # 為替損益を計算（出金時のJPY評価額 - 累積されたJPY雑所得）
    spread = jpy_evaluated - jpy_out  # スプレッドを計算（JPY残高の評価額 - 実際に出金されたJPY）

    # 出力レコード作成
    withdrawal_records.append(
        {
            "出金日": date,
            "USD出金額": usd_balance,
            "出金TTM": ttm_out,
            "JPY換算入金額（雑所得）": accumulated_jpy_income,
            "為替損益（雑所得）": fx_profit,
            "スプレッド（経費）": spread,
            "実際のJPY出金額": jpy_out,
            "JPY評価額（TTM換算）": jpy_evaluated,
        }
    )

    # 出金後にUSD残高と累積JPY所得をリセット（全額出金されたので、残高はゼロ）
    usd_balance = 0.0
    accumulated_jpy_income = 0.0

    return usd_balance, accumulated_jpy_income, withdrawal_records


# === 統合レポートの作成（入出金＋残高） ===
def create_merged_report(income_records: list, withdrawal_records: list, output_path: str) -> None:
    """入金・出金データを統合し、残高列を付加したCSVを出力

    入金と出金のレコードを統合し、各トランザクションの残高（USDおよびJPY換算）を計算して、
    最終的に統合されたCSVファイルを指定されたパスに出力します。

    Args:
        income_records (list): 入金トランザクションのリスト
        withdrawal_records (list): 出金トランザクションのリスト
        output_path (str): 統合されたレポートを保存するCSVファイルのパス
    """
    # 入金データの整形
    income_df = pd.DataFrame(income_records)
    income_df["種別"] = "入金"
    income_df = income_df.rename(
        columns={"入金日": "日付", "USD入金額": "USD金額", "入金時TTM": "TTM", "JPY換算額（雑所得）": "JPY換算額"}
    )
    income_df["為替損益"] = ""
    income_df["スプレッド"] = ""
    income_df["実際の出金額"] = ""

    # 出金データの整形
    withdraw_df = pd.DataFrame(withdrawal_records)
    withdraw_df["種別"] = "出金"
    withdraw_df = withdraw_df.rename(
        columns={
            "出金日": "日付",
            "USD出金額": "USD金額",
            "出金TTM": "TTM",
            "JPY換算入金額（雑所得）": "JPY換算額",
            "為替損益（雑所得）": "為替損益",
            "スプレッド（経費）": "スプレッド",
            "実際のJPY出金額": "実際の出金額",
        }
    )

    # 入出金を統合
    columns = ["種別", "日付", "USD金額", "TTM", "JPY換算額", "為替損益", "スプレッド", "実際の出金額"]
    combined_df = pd.concat([income_df[columns], withdraw_df[columns]], ignore_index=True)
    combined_df = combined_df.sort_values("日付").reset_index(drop=True)

    # 残高計算
    usd_balance = 0.0
    for i, row in combined_df.iterrows():
        usd_amt = row["USD金額"]
        ttm = row["TTM"]
        kind = row["種別"]

        # 入金または出金に応じてUSD残高を更新
        if kind == "入金":
            usd_balance += usd_amt
        elif kind == "出金":
            usd_balance -= usd_amt

        # 残高（USD・JPY換算）を更新
        combined_df.at[i, "残高（USD）"] = usd_balance
        combined_df.at[i, "残高（JPY換算）"] = usd_balance * ttm

    # 統合データをCSVとして出力
    combined_df.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"✅ 統合レポート作成完了 → {output_path}")


# === メイン処理 ===
def main() -> None:

    # ファイル設定
    transactions_file = "transactions.csv"
    ttm_file = "ttm_rates.csv"

    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, "tax_report.csv")
    income_detail_file = os.path.join(output_dir, "income_details.csv")
    merged_report_file = os.path.join(output_dir, "merged_report.csv")

    # データ読み込み
    ttm_df = load_ttm(ttm_file)
    df = load_transactions(transactions_file)

    # 初期状態
    usd_balance = 0.0
    accumulated_jpy_income = 0.0
    withdrawal_records = []
    income_records = []

    # 日付順でトランザクションを処理
    for _, row in df.sort_values("日付").iterrows():
        if row["通貨"] == "USD" and row["残高への影響"] == "入金":
            usd_balance, accumulated_jpy_income = process_income(
                row, ttm_df, usd_balance, accumulated_jpy_income, income_records
            )
        elif row["通貨"] == "USD" and row["残高への影響"] == "引落し":
            usd_balance, accumulated_jpy_income, withdrawal_records = process_withdrawal(
                row, ttm_df, usd_balance, accumulated_jpy_income, withdrawal_records, df
            )

    # レポート出力
    pd.DataFrame(withdrawal_records).to_csv(output_file, index=False, encoding="utf-8-sig")
    pd.DataFrame(income_records).to_csv(income_detail_file, index=False, encoding="utf-8-sig")

    print("処理完了 ✅")
    print(f"→ 出金レポート: {output_file}")
    print(f"→ 入金明細レポート: {income_detail_file}")

    # 統合レポート作成
    create_merged_report(income_records, withdrawal_records, merged_report_file)


# 実行エントリーポイント
if __name__ == "__main__":
    main()
