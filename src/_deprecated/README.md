# _deprecated/

v4.5 기준 더 이상 import되지 않는 구버전 모듈 보관소.

| 파일 | 마지막 사용 app 버전 | 후속 파일 |
|---|---|---|
| `scoring.py` | v4.0, v4.1 | `scoring_Ver4_3.py` |
| `scoring_Ver4_2.py` | v4.2 | `scoring_Ver4_3.py` |
| `visualizer.py` | v4.0 | `visualizer_Ver4_2.py` |
| `visualizer_Ver4_1.py` | v4.1 | `visualizer_Ver4_2.py` |

**주의:** 이 폴더의 파일을 import하려면 `src._deprecated.<모듈명>` 경로로 접근해야 하므로, 구 app 버전(v4.0~v4.2)은 이대로 실행하면 ImportError 발생. 과거 동작이 필요하면 해당 커밋을 체크아웃할 것.
