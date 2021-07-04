import sys


def main():
    kw = sys.argv[1]
    ret = []
    for ch in kw:
        ret.append('[')
        ret.append(ch.lower())
        ret.append(ch.upper())
        ret.append(']')
    print("".join(ret))
    return


if __name__ == '__main__':
    main()
