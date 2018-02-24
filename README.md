# AI Pipeline
A pipeline package for industrial data analysis and machine learning.

## 개발 환경 설정
### Anaconda 설치
### Env 생성

```bash
$ conda create -n pipe3 python=3 jupyter pandas matplotlib
$ source activate pipe3
(pipe3) $
```

### pymongo 설치(mongoDB 연결)

```
(pipe3) $ conda install pymongo
```

### psycopg2 설치(postgreSQL 연결)

```
(pipe3) $ conda install psycopg2
```

### luigi 설치

```
(pipe3) $ conda install luigi
```