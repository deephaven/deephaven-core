class MyNumbers:
    def __init__(self):
        pass

    def get_byte(self):
        return 2 ** 7 - 1

    def get_short(self):
        return 2 ** 15 - 1

    def get_int(self):
        return 2 ** 31 - 1

    def get_long(self):
        return 2 ** 63 - 1

    def get_bigint(self):
        return 2 ** 127 - 1
