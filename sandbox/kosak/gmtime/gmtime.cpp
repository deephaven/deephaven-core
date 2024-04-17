#include <ctime>
#include <iomanip>
#include <sstream>
#include <iostream>

const char* stupid(const char* s,
    struct tm* tm) {
    constexpr const char* kFormatToUse = "%Y-%m-%dT%H:%M:%S%z";
    std::istringstream input(s);
    input.imbue(std::locale(setlocale(LC_ALL, nullptr)));
    input >> std::get_time(tm, kFormatToUse);
    if (input.fail()) {
        return nullptr;
    }
    return (char*)(s + input.tellg());
}

int main()
{
    std::tm tm = {};
    const char* s = stupid("2013-03-01T12:34:56-0500", &tm);
    std::cout << "zamboni " << s << '\n';
    return 0;
}