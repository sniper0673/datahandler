import pandas as pd
import numpy as np

"""
주식의 상하한가를 계산하는 함수
"""

# 현재 시장의 상하한가 계산
def calculate_limit_current(price: pd.Series, side: str, after: bool = False) -> int:
    """
    side: 'buy' or 'sell'
    """
    if side == 'buy':
        return calculate_upper_limit(price, after)
    else:
        return calculate_lower_limit(price, after)
def calculate_upper_limit(price: pd.Series, after: bool = False) -> int:
    if not after:
        # 장중 상한가 계산
        return round_down_price_with_series_current(price * 1.3)
    else:
        # 시간외 상한가 계산
        return round_down_price_with_series_current(price * 1.1)
def calculate_lower_limit(price: pd.Series, after: bool = False) -> int:
    if not after:
        # 장중 하한가 계산
        return round_up_price_with_series_current(price * 0.7)
    else:
        # 시간외 하한가 계산
        return round_up_price_with_series_current(price * 0.9)

# 가격 올림처리
def round_up_price_with_series_current(prices: pd.Series) -> pd.Series:
    # 각 구간에 해당하는 step 값을 정의
    bins = [-float('inf'), 2000, 5000, 20000, 50000, 200000, 500000, float('inf')]
    steps = [1, 5, 10, 50, 100, 500, 1000]
    
    # pd.cut을 사용해 각 price에 대해 적절한 step 값을 할당
    step_series = pd.cut(prices, bins=bins, labels=steps, right=False).astype(int)
    
    # 각 price를 step에 맞춰 올림 처리
    rounded_prices = ((np.ceil(prices) + step_series - 0.1) // step_series) * step_series
    return rounded_prices
def round_up_price_with_series_kospi_current(prices: pd.Series) -> pd.Series:
    return round_up_price_with_series_current(prices)
def round_up_price_with_series_kosdaq_current(prices: pd.Series) -> pd.Series:
    return round_up_price_with_series_current(prices)

def round_up_price_with_series_kospi_old(prices: pd.Series) -> pd.Series:
    """
    과거 Kospi 시장의 가격 구간에 따른 올림 반올림 처리.
    """
    # 과거 Kospi 시장에서 각 구간에 해당하는 step 값을 정의
    bins = [-float('inf'), 1000, 5000, 10000, 50000, 100000, 500000, float('inf')]
    steps = [1, 5, 10, 50, 100, 500, 1000]
    
    # pd.cut을 사용해 각 price에 대해 적절한 step 값을 할당
    step_series = pd.cut(prices, bins=bins, labels=steps, right=False).astype(int)
    
    # 각 price를 step에 맞춰 올림 처리
    rounded_prices = ((np.ceil(prices) + step_series - 0.1) // step_series) * step_series
    return rounded_prices
def round_up_price_with_series_kosdaq_old(prices: pd.Series) -> pd.Series:
    """
    과거 Kosdaq 시장의 가격 구간에 따른 올림 반올림 처리.
    """
    # 과거 Kosdaq 시장에서 각 구간에 해당하는 step 값을 정의
    bins = [-float('inf'), 1000, 5000, 10000, 50000, float('inf')]
    steps = [1, 5, 10, 50, 100]
    
    # pd.cut을 사용해 각 price에 대해 적절한 step 값을 할당
    step_series = pd.cut(prices, bins=bins, labels=steps, right=False).astype(int)
    
    # 각 price를 step에 맞춰 올림 처리
    rounded_prices = ((np.ceil(prices) + step_series - 0.1) // step_series) * step_series
    return rounded_prices

# 가격 내림처리
def round_down_price_with_series_current(prices:pd.Series)->pd.Series:
    # 각 구간에 해당하는 step 값을 정의
    bins = [-float('inf'), 2000, 5000, 20000, 50000, 200000, 500000, float('inf')]
    steps = [1, 5, 10, 50, 100, 500, 1000]
    
    # pd.cut을 사용해 각 price에 대해 적절한 step 값을 할당
    step_series = pd.cut(prices, bins=bins, labels=steps, right=False).astype(int)
    
    # 각 price를 step에 맞춰 반올림 (내림)
    rounded_prices = (prices // step_series) * step_series
    return rounded_prices
def round_down_price_with_series_kospi_current(prices:pd.Series)->pd.Series:
    return round_down_price_with_series_current(prices)
def round_down_price_with_series_kosdaq_current(prices:pd.Series)->pd.Series:
    return round_down_price_with_series_current(prices)

def round_down_price_with_series_kospi_old(prices:pd.Series)->pd.Series:
    """
    과거 Kospi 시장의 가격 구간에 따른 내림 반올림 처리.
    """
    # 과거 Kospi 시장에서 각 구간에 해당하는 step 값을 정의
    bins = [-float('inf'), 1000, 5000, 10000, 50000, 100000, 500000, float('inf')]
    steps = [1, 5, 10, 50, 100, 500, 1000]
    
    # pd.cut을 사용해 각 price에 대해 적절한 step 값을 할당
    step_series = pd.cut(prices, bins=bins, labels=steps, right=False).astype(int)
    
    # 각 price를 step에 맞춰 반올림 (내림)
    rounded_prices = (prices // step_series) * step_series
    return rounded_prices
def round_down_price_with_series_kosdaq_old(prices:pd.Series)->pd.Series:
    """
    과거 Kosdaq 시장의 가격 구간에 따른 내림 반올림 처리.
    """
    # 과거 Kosdaq 시장에서 각 구간에 해당하는 step 값을 정의
    bins = [-float('inf'), 1000, 5000, 10000, 50000, float('inf')]
    steps = [1, 5, 10, 50, 100]
    
    # pd.cut을 사용해 각 price에 대해 적절한 step 값을 할당
    step_series = pd.cut(prices, bins=bins, labels=steps, right=False).astype(int)
    
    # 각 price를 step에 맞춰 반올림 (내림)
    rounded_prices = (prices // step_series) * step_series
    return rounded_prices

# float 값을 시리즈로 변환하여 계산
def round_up_price_current(price:float)->int:
    price_as_series = pd.Series(price)
    return int(round_up_price_with_series_current(price_as_series)[0])
def round_down_price_current(price:float)->int:
    price_as_series = pd.Series(price)
    return int(round_down_price_with_series_current(price_as_series)[0])

# 하나의 값에 대한 계산(위의 함수를 이용하지 않음)
def round_down_price(price, market:str='kospi', when:str='current'):
    """
    2023.1.25일부로 수정됨.
    """
    if isinstance(price, (pd.DataFrame, pd.Series)):
        return price.apply(round_down_price, market=market, when=when)
    price_unit = get_price_unit(price, market, when)
    return (price // price_unit) * price_unit
def round_up_price(price, market:str='kospi', when:str='current'):
    if isinstance(price, (pd.DataFrame, pd.Series)):
        return price.apply(round_up_price)
    price_unit = get_price_unit(price, market, when)
    return ((np.ceil(price) + price_unit - 0.1) // price_unit) * price_unit
def get_price_unit(price, market, when) -> int:
    """
    2023.1.25일부로 수정됨.
    """
    if when == 'current':
        if price < 2000:
            step = 1  # 2,000원 미만일 때 1원 단위
        elif price < 5000:
            step = 5  # 2,000원 이상 5,000원 미만일 때 5원 단위
        elif price < 20000:
            step = 10  # 5,000원 이상 20,000원 미만일 때 10원 단위
        elif price < 50000:
            step = 50  # 20,000원 이상 50,000원 미만일 때 50원 단위
        elif price < 200000:
            step = 100  # 50,000원 이상 200,000원 미만일 때 100원 단위
        elif price < 500000:
            step = 500  # 200,000원 이상 500,000원 미만일 때 500원 단위
        else:
            step = 1000  # 500,000원 이상일 때 1,000원 단위
            
    elif market.lower() == 'kospi' or market == '코스피':
        if price < 1000:
            step = 1  # 1,000원 미만일 때 1원 단위
        elif price < 5000:
            step = 5  # 1,000원 이상 5,000원 미만일 때 5원 단위
        elif price < 10000:
            step = 10  # 5,000원 이상 10,000원 미만일 때 10원 단위
        elif price < 50000:
            step = 50  # 20,000원 이상 50,000원 미만일 때 50원 단위
        elif price < 100000:
            step = 100  # 50,000원 이상 200,000원 미만일 때 100원 단위
        elif price < 500000:
            step = 500  # 200,000원 이상 500,000원 미만일 때 500원 단위
        else:
            step = 1000  # 500,000원 이상일 때 1,000원 단위
    else: # 과거 Kosdaq인 경우
        if price < 1000:
            step = 1  # 1,000원 미만일 때 1원 단위
        elif price < 5000:
            step = 5  # 1,000원 이상 5,000원 미만일 때 5원 단위
        elif price < 10000:
            step = 10  # 5,000원 이상 10,000원 미만일 때 10원 단위
        elif price < 50000:
            step = 50  # 20,000원 이상 50,000원 미만일 때 50원 단위
        else:
            step = 100  # 50,000원 이상 200,000원 미만일 때 100원 단위
    return step