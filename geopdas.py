import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import pyproj
from shapely.geometry import Polygon, MultiPolygon

# DIM_REGION.csv 파일 불러오기
dim_region = pd.read_csv('data/DIM_REGION.csv', encoding='cp949')

# 중복된 열 제거
dim_region = dim_region.drop(['SIG_KOR_NM_SG', 'geometry'], axis=1)

dim_region.reset_index(drop=True, inplace=True)

# sig.shp 파일 불러오기
gdf = gpd.read_file('data/sig.shp', encoding='cp949')
gdf = gdf.rename(columns={'SIG_KOR_NM': 'SIG_KOR_NM_SG', 'SIG_ENG_NM': 'SIG_ENG_NM_SG'})

# 'SIG_CD' 열의 데이터 형식 일치시키기
dim_region['SIG_CD'] = dim_region['SIG_CD'].astype(str)

# 좌표계 설정
gdf.crs = 'epsg:5179'

# 좌표계 변환
gdf = gdf.to_crs('epsg:4326')

# SIG_CD를 기준으로 조인
merged_data = dim_region.merge(gdf, on='SIG_CD', how='left')

# GeoDataFrame으로 변환
merged_data = gpd.GeoDataFrame(merged_data, geometry='geometry')

# 결과 확인
print(merged_data.head())

# 시각화
merged_data.plot()

# pyplot 설정
plt.axis('equal')  # 축 비율을 동일하게 설정

# 시각화 출력
plt.show()

order_list = ["ID", "SIG_CD", "SIG_KOR_NM", "SIG_KOR_NM_SG",
              "SIG_ENG_NM_SG", "latitude", "longitude", "POINT", "geometry"]
merged_data = merged_data[order_list]
merged_data.reset_index(drop=True, inplace=True)
merged_data.to_csv('final_region.csv', index=False, encoding='cp949')
