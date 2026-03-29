#include <cstdint>
#include <iostream>
#include <sstream>
#include <vector>


struct RGB {
    uint8_t r, g, b;
};

class Console {
public:
    // 预定义 idleKV 主题色：深青到电紫
    static constexpr RGB ThemeCyan = {0, 255, 255};
    static constexpr RGB ThemePurple = {170, 0, 255};

    /**
     * @brief 打印带垂直渐变色的文本块
     */
    static void PrintGradient(std::string_view text, RGB start = ThemeCyan, RGB end = ThemePurple) {
        std::stringstream ss{std::string(text)};
        std::string line;
        std::vector<std::string> lines;
        while (std::getline(ss, line)) {
            lines.push_back(line);
        }

        for (size_t i = 0; i < lines.size(); ++i) {
            float ratio = lines.size() > 1 ? static_cast<float>(i) / (lines.size() - 1) : 1.0f;
            
            int r = static_cast<int>(start.r + (end.r - start.r) * ratio);
            int g = static_cast<int>(start.g + (end.g - start.g) * ratio);
            int b = static_cast<int>(start.b + (end.b - start.b) * ratio);

            // \x1b[38;2;R;G;Bm 是 ANSI 真彩色转义码
            std::cout << "\x1b[38;2;" << r << ";" << g << ";" << b << "m" 
                      << lines[i] << "\x1b[0m\n";
        }
    }

    /**
     * @brief 打印带图标的日志行 (加粗风格)
     */
    static void LogInfo(std::string_view msg) {
        std::cout << "\x1b[1;32m[INFO]\x1b[0m " << msg << std::endl;
    }

    static void LogSuccess(std::string_view msg) {
        std::cout << "\x1b[1;36m[✔]\x1b[0m " << msg << std::endl;
    }
};
