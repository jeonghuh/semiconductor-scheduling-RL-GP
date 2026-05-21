# Intelligent Semiconductor Scheduling via Reinforcement Learning and Genetic Programming

반도체 하이브리드 플로우 숍(Hybrid Flow Shop) 환경에서 공정 효율을 극대화하기 위해 강화학습(RL)과 유전 프로그래밍(GP)을 결합한 지능형 오퍼레이션 스케줄링 시스템입니다. 

기존의 단순 규칙 기반 스케줄링의 한계를 넘어, 복잡한 공정 제약 조건을 만족하면서 목적 함수를 최적화하는 동적 스케줄링 규칙(Dynamic Scheduling Rules)을 자동으로 생성합니다.

## 1. 프로젝트 개요

반도체 제조 공정은 대규모 설비가 병렬로 배치된 하이브리드 플로우 숍 구조를 가집니다. 본 프로젝트는 납기 준수율 향상을 목표로 합니다.

* **목적 함수**: 총 지연 시간(Total Tardiness)의 최소화
* **핵심 방법론**: 유전 프로그래밍을 통해 휴리스틱 우선순위 함수 구조를 진화시키고, 상위 5개 룰을 선택. 선택된 룰을 바탕으로 강화학습 에이전트가 공정 상태(State)에 따라 실시간으로 최적의 규칙 및 파라미터를 선택하는 결합 아키텍처

## 2. 공정 시스템 설계

### 시스템 기본 조건
모든 job은 도착 시간이 모두 다른 dynamic arrival 상황입니다.
전체 stage 수는 고정적으로, 모든 job에게 동일하게 적용됩니다.
각 stage별 machine은 모두 eligibility 제한이 없고, 같은 stage에 있는 machine이라면 모두 처리시간도 동일한 identical machine 상황을 가정합니다.

### 제약 조건
본 반도체 공정은 재진입(reentrance)과 대기시간 제약(queue time limits)이 존재하는 공정입니다. 
대기시간 제약 초과에 따라 공정은 재진입(rework)가 발생할 수 있으며, 이에 따라 job별로 공정 route가 변화합니다.

- 대기시간 제약: stage 2-4, 3-5에 존재하는 overlapped queue time limits 상황. 대기시간 초과 시, rework stage로 들어가며, 제품 품질을 고려해 최대 rework 횟수는 1회로 제한
- 재진입: 모든 작업물은 stage 1-5를 재진입해야 하는 공정. 즉 stage 1-5는 총 2번 진입


### 유전 프로그래밍 (GP) 기반 규칙 생성
수학적 연산자(+, -, \*, /)와 공정 변수(원자재 대기 시간, 설비 셋업 시간 등)를 터미널 노드로 설정하여 Total Tardiness를 최소화 하는 방향으로 진화시킵니다.

유전 진화 과정에는 crossover, mutation이 존재하며, 기존 개체 수 40개에서 crossover, mutation된 개체, 그리고 새로운 랜덤 생성 40개체를 모두 포함한 개체에서 상위 40 개체만 다음 세대로 진화합니다.

- Crossover: 두 트리가 각각의 서브트리를 서로 교환
- Mutation: 한 트리의 서브트리를 다른 트리의 서브트리 부분에 이식


## 3. 알고리즘 평가

### 우선순위 규칙 생성 
- Train: Jobs=40,80,100 / Stages=7,15 / Machine=2,4 / instance=10
- Test: Jobs=40,60,80,100,150,200 / Stages=5,7,10,15 / Machine=2,4 / instance=10


유전 프로그래밍을 통해서 우선순위 규칙을 생성하는 과정은 train 부분입니다.
생성된 우선순위 규칙은 마지막 진화 개체수 만큼인 40개가 도출됩니다.

이후 test 파라미터 데이터를 활용하여 단일 우선순위 규칙을 사용하였을 때 total tardiness를 계산하였습니다. 그때, 가장 우수한 규칙에 voting을 하여 최종 voting 상위 5개의 규칙을 선정하여 강화학습의 action으로 사용하였습니다. 

### 강화학습 DQN 
강화학습 방법론으로는 Deep Q-Network를 사용하였습니다. 
<img width="1856" height="802" alt="image" src="https://github.com/user-attachments/assets/b0e33094-c6ab-4862-a767-70b854106668" />

<img width="1890" height="918" alt="image" src="https://github.com/user-attachments/assets/091bce23-1576-428b-ae03-b94f58c3675d" />

