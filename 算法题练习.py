import sys
def main():
    s = sys.stdin.read().strip()
    x = 0
    y = 0
    commands = s.split(';')
    for cmd in commands:
        if not cmd:
            continue
        if len(cmd) < 2 or len(cmd) > 3:
…           continue
        direction = cmd[0]
        distance_str = cmd[1:]
        if direction not in ['A', 'D', 'W', 'S']:
            continue
        if not distance_str.isdigit():
            continue
        distance = int(distance_str)
        if distance <= 0 or distance >= 100:
            continue
        if direction == 'A':
            x -= distance
        elif direction == 'D':
            x += distance
        elif direction == 'W':
            y += distance
        elif direction == 'S':
            y -= distance
    print(f"{x},{y}")
if __name__ == '__main__':
    main()