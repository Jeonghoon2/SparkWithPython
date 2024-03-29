## 상황 가정
- **데이터 분석 및 판매 추세 파악 시나리오 :**
  - 소매업체는 다양한 매장에서의 제품별 일일 판매 데이터를 분석하여 판매 추세를 파악하고, 인기 있는 제품을 식별하려고 합니다.
  - 이를 위해, JSON 형식으로 저장된 판매 데이터를 Apache Spark를 사용하여 처리하고 분석할 계획입니다.
  
## 시나리오 세부 사항
- **데이터 로드 :**
    - sales_data.json 파일에는 매장 ID(Store_ID), 제품 ID(Product_ID), 판매 날짜(Date), 판매 수량(Quantity), 제품 가격(Price)이 포함된 판매 데이터가 저장되어 있습니다.
    - 이 데이터는 라인 별 JSON 형식으로 되어 있으며, Apache Spark를 사용하여 읽어옵니다.
  
- **데이터 처리 :**
  - 로드된 데이터는 판매 추세 분석, 가장 많이 판매된 제품 식별, 매장별 판매량 분석 등 다양한 분석을 위해 처리됩니다.

- **데이터 적재 및 조회 :**
  - 처리된 데이터는 인-메모리 테이블에 적재되며, SQL 쿼리를 통해 조회 및 분석이 이루어집니다.

## 테스트 목적
- **데이터 로딩 및 적재 검증 :**
  - JSON 파일이 정확히 로드되어 각 레코드가 올바르게 데이터프레임으로 변환되는지 확인합니다.
  - 로드된 데이터가 인-메모리 테이블에 정확히 적재되고 조회되는지 검증합니다.