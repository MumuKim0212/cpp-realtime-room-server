# realtime-room-server

C++로 epoll 리액터를 구현한 실시간 룸 서버. 다수의 클라이언트가 룸에 접속해 메시지를 주고받는다.

> 로그인 → 룸 입장 → 채팅 브로드캐스트 → 퇴장까지 동작  
> 500 연결에서 초당 10만 건을 전달하며 브로드캐스트 지연 p99가 0.6ms  
> 타이머 큐와 `Room::update(dt)` 자리를 만들어두어 필요에 따라 룸에 20Hz 틱을 얹어 게임 상태 동기화로 확장 가능

관련 문서: [설계](docs/DESIGN.md) · [와이어 프로토콜](docs/PROTOCOL.md) · [벤치마크](docs/BENCHMARK.md)

## 아키텍처

이벤트 루프 스레드 하나가 상자 안의 모든 것을 소유하기 때문에 세션과 룸을 락 없이 관리할 수 있다.

```
        client   client   client
           \       |       /
            \      |      /            TCP
             v     v     v
    +-----------------------------------------+
    |  Acceptor                               |
    |     |                                   |
    |     v                                   |
    |  Session  --frame-->  Server  -->  Room |
    |     ^                              |    |
    |     +---------- send() ------------+    |
    |                                         |
    |  TimerQueue --> epoll_wait timeout      |
    +-----------------------------------------+
           |  submit()          ^  post()
           v                    |
        DB workers  --libpq-->  PostgreSQL
```

- **Acceptor**: 리스닝 소켓. backlog가 마를 때까지 받아낸다.
- **Session**: 연결 하나. 바이트 스트림을 길이-프리픽스 프레임으로 자르고, 부분 write를 버퍼에 쌓고, 느린 상대를 끊는다.
- **Server**: 조립 지점. 프레임 타입을 보고 로그인·입장·퇴장·채팅으로 보낸다.
- **Room**: 같은 패킷을 받는 사람들의 집합. "채팅 전송"이 아니라 "룸 전원에게 패킷 전송"으로 정의돼 있어서, 틱 기반 상태 동기화로 확장하면 상태 스냅샷도 같은 경로로 나간다.
- **TimerQueue**: `epoll_wait`의 타임아웃을 가장 가까운 만료 시각에서 계산한다. 유휴 타임아웃이 지금 이걸 쓰고, 20Hz 틱을 얹을 때도 같은 자리에 들어온다.

> 스레드 경계는 이벤트 루프와 DB 워커 풀 사이의 하나뿐이다. 나갈 때는 `submit()`, 돌아올 때는 `post()`로 넘어가며, 서버 코드 전체에서 뮤텍스는 이 경계에만 있다.

### CHAT_REQ 한 건의 전달 경로

1. `Session`이 소켓에서 읽은 바이트를 프레임으로 자른다. 한 번의 `recv()`에 프레임이 잘려 오거나 여러 개가 뭉쳐 와도 여기서 정리된다.
2. `Server`가 타입을 보고 `handle_chat`으로 보낸다. 로그인 상태와 룸 소속을 확인하고, 길이 상한을 넘으면 연결은 살린 채 `ERROR_NTF`로 답한다.
3. `Room`이 멤버 전원에게 `CHAT_NTF`를 보낸다. **보낸 사람도 포함**이라 모두가 서버가 정한 하나의 순서로 본다. 각자가 자기 메시지를 끼워 넣은 순서를 추측하지 않는다.
4. 그러고 나서야 DB 워커에 로그 쓰기를 던진다. 이미 눈앞에 나간 메시지를 디스크 쓰기가 붙들지 않는다

## 설계 결정

코드를 읽을 때 먼저 마주칠 결정들이다. 전체 목록과 근거, 각 결정을 지키는 테스트는 [설계](docs/DESIGN.md) 문서에 있다.

- **epoll 리액터 직접 구현.** Boost.Asio를 쓰면 이 프로젝트가 다루려던 것(스케줄링, 버퍼 관리, 백프레셔)이 전부 라이브러리 안으로 사라지므로 세부 기능을 코드로 직접 구현했다.
- **틱 스레드를 두지 않았다.** 별도 스레드가 룸 상태를 만지면 락이 필요해지고, 그 락이 브로드캐스트와 얽힌다. 타이머를 루프에 넣어 `epoll_wait` 타임아웃으로 만료를 기다리면 I/O와 시간이 한 스레드에 공존한다. `timerfd`도 가능하지만 20Hz(50ms)에는 밀리초 해상도로 충분해서 fd 하나와 타이머 재설정의 syscall을 아꼈다.
- **엣지 트리거를 쓰지 않는다.** 레벨 트리거로 등록하고 알림당 `recv()`를 한 번만 부른다. ET로 `EAGAIN`까지 드레인하면 바쁜 연결 하나가 나머지 전부를 기다리게 만들 수 있다. 남은 데이터는 루프가 다시 알려주므로 이벤트는 잃지 않는다.
- **백프레셔는 통지 없이 끊는다.** 송신 버퍼가 1MiB를 넘으면 연결을 종료한다. `ERROR_NTF`를 먼저 보내지 않는 이유는 그 한도에 닿았다는 것 자체가 상대가 소켓을 비우지 않는다는 뜻이라, 통지가 이미 너무 긴 큐에 한 줄을 더 얹을 뿐이기 때문이다.
- **길이 필드가 u16이다.** 한 패킷이 형식상 65535바이트를 넘을 수 없다. 거대한 할당을 유발하는 공격을 코드의 검사가 아니라 형식이 막는다.
- **모든 큐에 상한을 둔다.** 송신 버퍼 1 MiB, DB 워커 큐 8192개, 연결 4096개, 종료 드레인 2초(소켓)와 3초(DB 큐). 상한 없는 큐는 빠른 실패를 느린 실패로 바꿀 뿐이고, 어디서 터질지 고를 권리까지 같이 버린다. 넘칠 때 무엇을 하는지는 큐마다 다르다. 1) 채팅 로그는 아무도 기다리지 않으니 세면서 버리고, 2) 로그인은 "지금은 확인해줄 수 없다"로 답하고, 3) 새 연결은 세션을 만들기 전에 거절한다.
- **세션 해체를 콜백 도중에 하지 않는다.** 브로드캐스트가 수신자 목록을 걷는 중에 그중 하나가 끊기면 그 순회가 무너진다. `close()`는 표시만 하고 실제 해체는 0ms 타이머로 루프에 넘긴다.
- **TLS는 서버 밖에서 종단한다.** 서버는 평문 TCP로 통신하고, 전송 구간 암호화가 필요한 환경에서는 앞단의 프록시(nginx 등)가 TLS를 맡는다. 논블로킹 소켓에서 TLS를 처리하면 읽기와 쓰기가 서로 얽혀서, 이 프로젝트가 드러내려는 송수신 경로가 핸드셰이크 상태 기계로 바뀌기 때문이다. 비밀번호는 DB에 Argon2id 해시로만 저장한다.

## 벤치마크

500 연결·룸당 100명 구성에서 초당 10만 건을 전달하며, 브로드캐스트 지연은 p50 0.16ms, p99 0.60ms를 기록했다. 측정 조건과 전체 수치, 그리고 이 측정이 실제로 찾아낸 결함(`TCP_NODELAY` 누락으로 p99가 28.8ms까지 벌어졌던 문제와 DB 워커 큐 상한 이슈)은 [BENCHMARK.md](docs/BENCHMARK.md)에 있다.

## 요구사항

- Linux (epoll 기반, 다른 플랫폼은 Docker 사용)
- CMake 3.20 이상
- C++20 지원 컴파일러 (GCC 13+ / Clang 16+)
- pkg-config, libsodium (비밀번호 해시), libpq (PostgreSQL 클라이언트)

```sh
sudo apt install build-essential cmake pkg-config libsodium-dev libpq-dev
```

## 실행 (Docker)

서버와 PostgreSQL을 한 번에 띄운다. 스키마는 첫 기동 때 한 번 적재된다.

```sh
docker compose -f docker/docker-compose.yml up --build
```

`9000` 포트로 붙으면 된다. `docker compose down -v`는 DB 볼륨까지 지워 스키마를 다시 심는다.

## 계정 만들기

스키마는 룸 셋(`lobby`/`general`/`random`)을 심지만 **계정은 심지 않는다.** 프로토콜에 회원가입 메시지가 없는 이유도 누가 계정을 갖는지는 운영자의 일이지 클라이언트가 요청할 일이 아니라고 판단했다. 그래서 서버 바이너리가 그 하나를 겸한다.

```sh
echo -n 'correct horse' | docker compose -f docker/docker-compose.yml \
    exec -T server room_server --create-user alice
```

비밀번호를 argv가 아니라 stdin으로 받는 이유는 프로세스 목록과 셸 히스토리에 남지 않게 하기 위해서다. 입력할 때 가려지지는 않으므로 위처럼 파이프로 넣도록 제작했다. 이름과 비밀번호는 프로토콜과 같은 상한(32바이트 / 128바이트)으로 검사해서, 로그인이 실어 나르지 못할 계정을 만들어두는 일이 없도록 했다.

직접 빌드한 경우에도 같다.

```sh
echo -n 'correct horse' | RRS_DATABASE_URL="..." ./build/src/room_server --create-user alice
```

## 빌드 (직접)

```sh
cmake -B build
cmake --build build
./build/src/room_server
```

테스트를 제외하려면 (GoogleTest를 내려받지 않음):

```sh
cmake -B build -DRRS_BUILD_TESTS=OFF
```

## 테스트

```sh
cmake -B build
cmake --build build
ctest --test-dir build --output-on-failure
```

데이터베이스 테스트는 실제 PostgreSQL이 필요하다. `RRS_TEST_DATABASE_URL`이 없으면 해당 테스트만 건너뛰고 나머지는 그대로 돈다.

```sh
docker run -d --name rrs-pg -p 5432:5432 \
    -e POSTGRES_PASSWORD=rrs -e POSTGRES_DB=rrs \
    -v "$PWD/docker/schema.sql:/docker-entrypoint-initdb.d/schema.sql:ro" \
    postgres:16

RRS_TEST_DATABASE_URL="host=127.0.0.1 port=5432 user=postgres password=rrs dbname=rrs" \
    ctest --test-dir build --output-on-failure
```

## 부하 테스트

`tools/loadtest/`의 클라이언트가 N개 연결을 붙여 룸에 나눠 넣고, 서로에게 채팅을 보내며 **보낸 쪽이 소켓에 쓴 시각과 받은 쪽이 읽어낸 시각의 차**를 전달 건마다 잰다.

계정과 룸을 먼저 심어야 한다. 서버는 룸 목록을 기동할 때 한 번만 읽으므로, seeding은 서버가 뜨기 전에 끝나 있어야 한다.

```sh
./build/tools/loadtest/room_loadtest --seed --clients=500 --rooms=5 \
    --database-url="host=127.0.0.1 port=5432 user=rrs password=rrs dbname=rrs"
```

Seeding은 `load000000`부터 계정을 만들고, `9001`번부터 실행 크기에 맞는 capacity의 룸을 만든다.
스키마가 심어두는 `lobby`/`general`/`random`은 정원이 64라 큰 실행에 쓸 수 없어서 건드리지 않고 따로 만든다.
비밀번호 해시가 의도적으로 느리게 제작되었기 때문에 이미 존재하는 계정은 다시 만들지 않고 새로운 계정만 생성한다.

서버를 기동한 뒤 부하를 건다.

```sh
./build/tools/loadtest/room_loadtest --clients=500 --rooms=5 --rate=1 --duration=30
```

| 옵션 | 기본값 | 설명 |
|---|---|---|
| `--host` `--port` | `127.0.0.1` `9000` | 붙을 서버 |
| `--clients` | `100` | 연결 수 |
| `--rooms` | `3` | 나눠 들어갈 룸 수 |
| `--rate` | `1` | 클라이언트당 초당 메시지 |
| `--duration` | `30` | 측정 시간(초) |
| `--size` | `64` | 채팅 텍스트 바이트 |

전원이 입장할 때까지 기다린 뒤에 계측을 시작한다. 로그인마다 Argon2id 검증이 붙어서 마지막 연결은 첫 연결보다 몇 초 늦게 들어오는데, 그 구간을 같이 재면 램프업을 재는 것이 된다.

측정 수치는 서버뿐 아니라 그것을 돌리는 환경도 반영하므로, 실행마다 조건이 수치와 함께 출력된다. 실제로 나온 수치는 [벤치마크](docs/BENCHMARK.md) 문서에 있다.

주의: 테스트 스위트의 `DatabaseTest`는 `users` 테이블을 `TRUNCATE`한다. 부하용 계정을 심어둔 데이터베이스를 `RRS_TEST_DATABASE_URL`로 물리면 계정이 지워지고, 다음 부하 실행은 로그인이 전부 거부된다.

## 디렉토리 구조

```
src/
  main.cpp        진입점, 환경변수 설정 읽기
  server.cpp      조립 지점. 메시지 디스패치, 클라이언트 레지스트리
  net/            epoll 리액터, 타이머 큐, 리스닝 소켓
  session/        연결 상태, 송수신 버퍼링, 백프레셔
  room/           룸 생명주기, 브로드캐스트, 틱 업데이트
  db/             비동기 DB 워커 풀, PostgreSQL, 비밀번호 해시
  protocol/       길이-프리픽스 바이너리 패킷
tests/            유닛 테스트 + 종단 테스트
tools/loadtest/   부하 테스트 클라이언트
docker/           schema.sql, Dockerfile, docker-compose.yml
docs/
  DESIGN.md       설계 결정과 근거, 알려진 한계
  PROTOCOL.md     와이어 프로토콜 규격
  BENCHMARK.md    벤치마크 조건과 수치
```

## 설정

설정값은 환경변수로 받는다.

| 변수 | 기본값 | 설명 |
|---|---|---|
| `RRS_LISTEN_HOST` | `0.0.0.0` | 바인딩 주소 (IPv4) |
| `RRS_LISTEN_PORT` | `9000` | 바인딩 포트 |
| `RRS_DATABASE_URL` | *(필수)* | libpq 연결 문자열 |
| `RRS_DATABASE_WORKERS` | `4` | DB 워커 스레드 수 |
| `RRS_MAX_CONNECTIONS` | `4096` | 동시에 쥘 연결 수 상한 |

`RRS_DATABASE_WORKERS`를 올릴 때는 메모리를 같이 본다. Argon2id 검증 한 번이 **64 MiB**를 쓰고 그게 워커마다 동시에 일어날 수 있어서, 기본값 4는 로그인이 몰리는 순간 약 270 MB를 잡는다(실측). 32로 올리면 2 GiB를 예산에 넣어야 한다.

`RRS_MAX_CONNECTIONS`는 프로세스의 파일 디스크립터 한도보다 아래여야 하며, 그 위에 리스닝 소켓, DB 연결 N개, signalfd가 더 필요하다. 서버는 기동할 때 soft limit이 모자라면 hard limit 안에서 필요한 만큼 올리고, hard limit으로도 모자라면 시작하지 않는다. 정원을 넘었을 때 새로 들어오는 연결은 세션을 만들지 않고 `ERROR_NTF(1006)`을 받고 닫힌다.

메모리도 같이 본다. **송신 버퍼 상한 1 MiB는 "상대에게 빚진 바이트"의 상한이지 잡아둔 메모리의 상한이 아니다.** 소비된 앞부분을 바로 회수하지 않으므로 담고 있는 범위가 그 두 배까지 가고, 그것을 담는 vector가 자랄 때 배로 잡으므로 실제 예약은 그 다시 두 배까지 간다. 한도에 닿은 연결 하나는 1 MB가 아니라 4 MB에 가깝다. 또한, **연결 전체를 합친 상한은 없다.** 상한은 연결당이므로 최악의 경우는 정원 × 연결당이고, 기본값 4096이면 16 GB다. 그 최악은 4096개가 동시에 정체돼야 나오지만, 정원을 정할 때 예산에 넣어야 하는 쪽은 그 곱이다.

`SIGINT` / `SIGTERM`을 받으면 새 연결과 새 요청을 받는 것을 먼저 멈추고, 이미 클라이언트 앞에 쌓여 있던 송신 버퍼를 최대 2초 안에서 내보낸 뒤 종료한다. 읽지 않는 피어가 종료를 붙들지 못하도록 이 2초는 상한이며, 신호를 한 번 더 보내면 기다리지 않고 끝낸다.

DB 워커 큐에 남아 있던 쓰기(채팅 로그 등)도 **최대 3초 안에서** 끝낸 뒤 join한다. 이쪽에도 상한이 있는 이유는 없을 때 무슨 일이 벌어지는지 재봤기 때문이다. 백로그를 안은 채로는 종료에 168초가 걸렸고, `docker stop`이 주는 유예는 10초다. 상한 없이 "다 끝내고 나간다"는 것은 실제로는 SIGKILL로 잘려 **전부** 잃는다는 뜻이었다. 예산을 넘긴 잔여는 개수와 함께 로그에 남기고 버린다.

결과적으로 전체 종료 시간의 상한은 2초(소켓) + 3초(DB 큐) + 실행 중인 태스크 하나다. 룸 목록을 읽지 못해 시작에 실패하면 종료 코드 1로 끝난다.
