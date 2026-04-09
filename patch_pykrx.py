"""
pykrx 소스 패치 스크립트
KRX API가 지수값을 float 문자열로 반환하면서 pykrx 내부 astype(int) 오류 발생
→ pykrx 소스의 astype(int) → astype(float) 로 교체
"""
import os
import pykrx

pkg_path = os.path.dirname(pykrx.__file__)
print(f"[패치] pykrx 경로: {pkg_path}")

patched = 0
for root, dirs, files in os.walk(pkg_path):
    for filename in files:
        if not filename.endswith('.py'):
            continue
        filepath = os.path.join(root, filename)
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()

        new_content = content
        new_content = new_content.replace('astype(int)',  'astype(float)')
        new_content = new_content.replace(".apply(int)",  ".apply(lambda x: int(float(str(x).replace(',',''))) if str(x).replace('.','').replace(',','').replace('-','').isdigit() or ('.' in str(x)) else 0)")

        if new_content != content:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(new_content)
            print(f"  ✅ 패치 완료: {filepath}")
            patched += 1

if patched == 0:
    print("  ℹ️  패치 대상 없음 (이미 패치됐거나 해당 코드 없음)")
else:
    print(f"[패치] 총 {patched}개 파일 수정 완료")
