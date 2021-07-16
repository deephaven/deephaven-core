class Column:
    def __init__(self, name, attrs={}):
        self.name = name
        self.attrs = attrs

    def __str__(self):
        lines = [str(self.name)]
        for attr in self.attrs.items():
            lines.append(str(attr))

        return "\n".join(lines)