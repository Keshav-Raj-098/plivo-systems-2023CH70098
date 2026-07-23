#include <iostream>
#include <vector>
#include <mutex>
#include <thread>
#include <atomic>
#include <cstring>
#include <cstdlib>

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <time.h>

struct FrameCache {
    std::mutex mtx;
    uint8_t payload[50000][160];
    bool valid[50000]{false};
    double last_sent_time[50000]{0.0};
};

static FrameCache g_cache;
static std::atomic<bool> g_running{true};
static uint8_t g_prev_even_payload[160];
static bool g_has_prev_even{false};

static inline double get_time_s_sender() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec * 1e-9;
}

void feedback_thread_func(int feedbackFd, int senderSocket, sockaddr_in relayAddress) {
    uint8_t buffer[2048];
    while (g_running.load()) {
        sockaddr_in fromAddr{};
        socklen_t fromLen = sizeof(fromAddr);
        ssize_t n = recvfrom(feedbackFd, buffer, sizeof(buffer), 0, (sockaddr *)&fromAddr, &fromLen);
        if (n <= 0) {
            continue;
        }

        double now = get_time_s_sender();

        // NACK packet format: [0x03 (1B)][count (2B uint16)][seq0 (2B uint16)]...
        if (n >= 3 && buffer[0] == 0x03) {
            uint16_t count;
            std::memcpy(&count, buffer + 1, sizeof(count));
            count = ntohs(count);

            for (uint16_t k = 0; k < count; ++k) {
                size_t offset = 3 + k * 2;
                if (offset + 2 <= (size_t)n) {
                    uint16_t seq16;
                    std::memcpy(&seq16, buffer + offset, sizeof(seq16));
                    uint32_t seq = ntohs(seq16);

                    if (seq < 50000) {
                        uint8_t retxPkt[163];
                        bool shouldSend = false;
                        {
                            std::lock_guard<std::mutex> lock(g_cache.mtx);
                            if (g_cache.valid[seq] && (now - g_cache.last_sent_time[seq] >= 0.020)) {
                                retxPkt[0] = 0x02; // retransmission type
                                uint16_t sNet = htons((uint16_t)seq);
                                std::memcpy(retxPkt + 1, &sNet, 2);
                                std::memcpy(retxPkt + 3, g_cache.payload[seq], 160);
                                g_cache.last_sent_time[seq] = now;
                                shouldSend = true;
                            }
                        }
                        if (shouldSend) {
                            sendto(senderSocket, retxPkt, 163, 0, (sockaddr *)&relayAddress, sizeof(relayAddress));
                        }
                    }
                }
            }
        }
    }
}

int main() {
    int senderSocket = socket(AF_INET, SOCK_DGRAM, 0);
    if (senderSocket < 0) {
        std::cerr << "Sender socket creation failed.\n";
        return 1;
    }

    sockaddr_in senderAddress{};
    senderAddress.sin_family = AF_INET;
    senderAddress.sin_port = htons(47010);
    senderAddress.sin_addr.s_addr = INADDR_ANY;

    if (bind(senderSocket, (sockaddr *)&senderAddress, sizeof(senderAddress)) < 0) {
        std::cerr << "Sender bind 47010 failed.\n";
        return 1;
    }

    int feedbackSocket = socket(AF_INET, SOCK_DGRAM, 0);
    if (feedbackSocket < 0) {
        std::cerr << "Feedback socket creation failed.\n";
        return 1;
    }

    sockaddr_in feedbackAddress{};
    feedbackAddress.sin_family = AF_INET;
    feedbackAddress.sin_port = htons(47004);
    feedbackAddress.sin_addr.s_addr = INADDR_ANY;

    if (bind(feedbackSocket, (sockaddr *)&feedbackAddress, sizeof(feedbackAddress)) < 0) {
        std::cerr << "Feedback bind 47004 failed.\n";
        return 1;
    }

    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = 100000;
    setsockopt(feedbackSocket, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    sockaddr_in relayAddress{};
    relayAddress.sin_family = AF_INET;
    relayAddress.sin_port = htons(47001);
    inet_pton(AF_INET, "127.0.0.1", &relayAddress.sin_addr);

    std::thread fbThread(feedback_thread_func, feedbackSocket, senderSocket, relayAddress);

    uint8_t buffer[2048];
    while (true) {
        ssize_t bytesReceived = recvfrom(senderSocket, buffer, sizeof(buffer), 0, NULL, NULL);
        if (bytesReceived < 164) {
            continue;
        }

        uint32_t seq;
        std::memcpy(&seq, buffer, sizeof(seq));
        seq = ntohl(seq);

        if (seq < 50000) {
            std::lock_guard<std::mutex> lock(g_cache.mtx);
            std::memcpy(g_cache.payload[seq], buffer + 4, 160);
            g_cache.valid[seq] = true;
            g_cache.last_sent_time[seq] = get_time_s_sender();
        }

        uint16_t seq16 = htons((uint16_t)seq);

        if (seq % 50 == 0) {
            // Primary packet only (163 bytes)
            uint8_t pkt[163];
            pkt[0] = 0x00;
            std::memcpy(pkt + 1, &seq16, 2);
            std::memcpy(pkt + 3, buffer + 4, 160);

            std::memcpy(g_prev_even_payload, buffer + 4, 160);
            g_has_prev_even = true;

            sendto(senderSocket, pkt, 163, 0, (sockaddr *)&relayAddress, sizeof(relayAddress));
        } else {
            // Primary + XOR parity of (seq-1, seq) (323 bytes)
            uint8_t pkt[323];
            pkt[0] = 0x01;
            std::memcpy(pkt + 1, &seq16, 2);
            std::memcpy(pkt + 3, buffer + 4, 160);

            if (g_has_prev_even) {
                for (int j = 0; j < 160; ++j) {
                    pkt[163 + j] = g_prev_even_payload[j] ^ buffer[4 + j];
                }
            } else {
                std::memset(pkt + 163, 0, 160);
            }

            std::memcpy(g_prev_even_payload, buffer + 4, 160);
            g_has_prev_even = true;

            sendto(senderSocket, pkt, 323, 0, (sockaddr *)&relayAddress, sizeof(relayAddress));
        }
    }

    g_running.store(false);
    if (fbThread.joinable()) {
        fbThread.join();
    }
    close(senderSocket);
    close(feedbackSocket);
    return 0;
}