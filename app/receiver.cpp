#include <iostream>
#include <vector>
#include <mutex>
#include <thread>
#include <atomic>
#include <chrono>
#include <cstring>
#include <cstdlib>

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <time.h>

struct FrameState {
    bool received{false};
    bool delivered{false};
    uint8_t payload[160];
    double last_nack_time{0.0};
};

static std::vector<FrameState> g_frames(50000);
static std::mutex g_framesMtx;
static std::atomic<int> g_maxSeqSeen{-1};
static std::atomic<bool> g_running{true};

static inline double get_time_s() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec * 1e-9;
}

void feedback_thread_func(int feedbackFd, sockaddr_in relayFbAddr, double t0, double delayMs) {
    while (g_running.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        double now = get_time_s();

        int maxSeq = g_maxSeqSeen.load();
        if (maxSeq < 0) {
            continue;
        }

        std::vector<uint16_t> missingSeqs;
        {
            std::lock_guard<std::mutex> lock(g_framesMtx);
            for (int i = 0; i <= maxSeq; ++i) {
                if (!g_frames[i].received && !g_frames[i].delivered) {
                    double deadline = t0 + (delayMs / 1000.0) + (i * 0.020);
                    if (now < deadline) {
                        double emitTime = t0 + (i * 0.020);
                        bool isGap = (i < maxSeq);
                        bool isOverdue = (now >= emitTime + 0.025);
                        if ((isGap || isOverdue) && (now - g_frames[i].last_nack_time >= 0.020)) {
                            g_frames[i].last_nack_time = now;
                            missingSeqs.push_back((uint16_t)i);
                            if (missingSeqs.size() >= 50) break;
                        }
                    }
                }
            }
        }

        if (!missingSeqs.empty()) {
            uint8_t pkt[512];
            pkt[0] = 0x03; // NACK type
            uint16_t count = (uint16_t)missingSeqs.size();
            uint16_t countNet = htons(count);
            std::memcpy(pkt + 1, &countNet, 2);

            for (size_t k = 0; k < missingSeqs.size(); ++k) {
                uint16_t seqNet = htons(missingSeqs[k]);
                std::memcpy(pkt + 3 + k * 2, &seqNet, 2);
            }

            size_t pktLen = 3 + missingSeqs.size() * 2;
            sendto(feedbackFd, pkt, pktLen, 0, (sockaddr *)&relayFbAddr, sizeof(relayFbAddr));
        }
    }
}

void playout_thread_func(int playerSocket, sockaddr_in playerAddress, double t0, double delayMs, int totalFrames) {
    for (int i = 0; i < totalFrames; ++i) {
        if (!g_running.load()) break;

        double deadline = t0 + (delayMs / 1000.0) + (i * 0.020);
        double targetPlayout = deadline - 0.002;

        while (g_running.load()) {
            double now = get_time_s();
            if (now >= targetPlayout) break;
            double sleepSec = targetPlayout - now;
            if (sleepSec > 0.001) {
                std::this_thread::sleep_for(std::chrono::microseconds((int64_t)(sleepSec * 1e6 * 0.7)));
            } else {
                std::this_thread::yield();
            }
        }

        bool delivered = false;
        uint8_t pktBuf[164];

        {
            std::lock_guard<std::mutex> lock(g_framesMtx);
            if (g_frames[i].received && !g_frames[i].delivered) {
                g_frames[i].delivered = true;
                uint32_t seqNet = htonl((uint32_t)i);
                std::memcpy(pktBuf, &seqNet, 4);
                std::memcpy(pktBuf + 4, g_frames[i].payload, 160);
                delivered = true;
            }
        }

        if (delivered) {
            sendto(playerSocket, pktBuf, 164, 0, (sockaddr *)&playerAddress, sizeof(playerAddress));
        } else {
            while (g_running.load()) {
                double now = get_time_s();
                if (now >= deadline) break;

                {
                    std::lock_guard<std::mutex> lock(g_framesMtx);
                    if (g_frames[i].received && !g_frames[i].delivered) {
                        g_frames[i].delivered = true;
                        uint32_t seqNet = htonl((uint32_t)i);
                        std::memcpy(pktBuf, &seqNet, 4);
                        std::memcpy(pktBuf + 4, g_frames[i].payload, 160);
                        delivered = true;
                    }
                }

                if (delivered) {
                    sendto(playerSocket, pktBuf, 164, 0, (sockaddr *)&playerAddress, sizeof(playerAddress));
                    break;
                }

                std::this_thread::sleep_for(std::chrono::microseconds(500));
            }
        }
    }
}

int main() {
    const char *t0_env = std::getenv("T0");
    const char *delay_env = std::getenv("DELAY_MS");
    const char *dur_env = std::getenv("DURATION_S");

    double t0 = t0_env ? std::atof(t0_env) : get_time_s();
    double delayMs = delay_env ? std::atof(delay_env) : 60.0;
    double durationS = dur_env ? std::atof(dur_env) : 30.0;
    int totalFrames = (int)(durationS * 1000.0 / 20.0);

    int receiverSocket = socket(AF_INET, SOCK_DGRAM, 0);
    if (receiverSocket < 0) {
        std::cerr << "Receiver socket creation failed.\n";
        return 1;
    }

    sockaddr_in receiverAddress{};
    receiverAddress.sin_family = AF_INET;
    receiverAddress.sin_port = htons(47002);
    receiverAddress.sin_addr.s_addr = INADDR_ANY;

    if (bind(receiverSocket, (sockaddr *)&receiverAddress, sizeof(receiverAddress)) < 0) {
        std::cerr << "Receiver bind 47002 failed.\n";
        return 1;
    }

    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = 20000;
    setsockopt(receiverSocket, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    sockaddr_in playerAddress{};
    playerAddress.sin_family = AF_INET;
    playerAddress.sin_port = htons(47020);
    inet_pton(AF_INET, "127.0.0.1", &playerAddress.sin_addr);

    int feedbackSocket = socket(AF_INET, SOCK_DGRAM, 0);
    sockaddr_in relayFbAddr{};
    relayFbAddr.sin_family = AF_INET;
    relayFbAddr.sin_port = htons(47003);
    inet_pton(AF_INET, "127.0.0.1", &relayFbAddr.sin_addr);

    std::thread fbThread(feedback_thread_func, feedbackSocket, relayFbAddr, t0, delayMs);
    std::thread playoutThread(playout_thread_func, receiverSocket, playerAddress, t0, delayMs, totalFrames);

    uint8_t buffer[2048];
    while (g_running.load()) {
        ssize_t bytesReceived = recvfrom(receiverSocket, buffer, sizeof(buffer), 0, NULL, NULL);
        if (bytesReceived < 163) {
            continue;
        }

        uint8_t pktType = buffer[0];
        uint16_t seq16;
        std::memcpy(&seq16, buffer + 1, 2);
        uint32_t seq = ntohs(seq16);

        if (seq < 50000) {
            std::lock_guard<std::mutex> lock(g_framesMtx);

            if (pktType == 0x00 || pktType == 0x02) {
                // Primary payload
                if (!g_frames[seq].received) {
                    g_frames[seq].received = true;
                    std::memcpy(g_frames[seq].payload, buffer + 3, 160);
                    int currMax = g_maxSeqSeen.load();
                    if ((int)seq > currMax) g_maxSeqSeen.store((int)seq);
                }
            } else if (pktType == 0x01 && bytesReceived >= 323) {
                // Primary payload + XOR parity of (seq-1, seq)
                if (!g_frames[seq].received) {
                    g_frames[seq].received = true;
                    std::memcpy(g_frames[seq].payload, buffer + 3, 160);
                    int currMax = g_maxSeqSeen.load();
                    if ((int)seq > currMax) g_maxSeqSeen.store((int)seq);
                }

                // Instantly recover previous frame seq-1 if missing
                if (seq > 0) {
                    uint32_t prevSeq = seq - 1;
                    if (!g_frames[prevSeq].received) {
                        for (int j = 0; j < 160; ++j) {
                            g_frames[prevSeq].payload[j] = buffer[3 + j] ^ buffer[163 + j];
                        }
                        g_frames[prevSeq].received = true;
                        int currMax = g_maxSeqSeen.load();
                        if ((int)prevSeq > currMax) g_maxSeqSeen.store((int)prevSeq);
                    }
                }
            }
        }
    }

    g_running.store(false);
    if (fbThread.joinable()) fbThread.join();
    if (playoutThread.joinable()) playoutThread.join();

    close(receiverSocket);
    close(feedbackSocket);
    return 0;
}