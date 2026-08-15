#ifndef BID_H
#define BID_H
#include <array>
#include <string>
#include "AES.hpp"
using namespace std;

class Bid {
public:

    /**
     * constant time selector
     * @param a
     * @param b
     * @param choice 0 or 1
     * @return choice = 1 -> a , choice = 0 -> return b
     */
    static int conditional_select(int a, int b, int choice) {
        unsigned int one = 1;
        unsigned int mask = ((unsigned int) choice - one);
        return (int) ((~mask & (unsigned int) a) | (mask & (unsigned int) b));
    }

    static bool CTeq(int a, int b) {
        return !(a^b);
    }

    static bool CTeq(long long a, long long b) {
        return !(a^b);
    }

    static bool CTeq(unsigned __int128 a, unsigned __int128 b) {
        return !(a^b);
    }

    static bool CTeq(unsigned long long a, unsigned long long b) {
        return !(a^b);
    }

    static int CTcmp(long long lhs, long long rhs) {
        unsigned __int128 overflowing_iff_lt = (unsigned __int128) ((__int128) lhs - (__int128) rhs);
        unsigned __int128 overflowing_iff_gt = (unsigned __int128) ((__int128) rhs - (__int128) lhs);
        int is_less_than = (int) -(overflowing_iff_lt >> 127); // -1 if self < other, 0 otherwise.
        int is_greater_than = (int) (overflowing_iff_gt >> 127); // 1 if self > other, 0 otherwise.
        int result = is_less_than + is_greater_than;
        return result;
    }

    /**
     * constant time selector
     * @param a
     * @param b
     * @param choice 0 or 1
     * @return choice = 1 -> a , choice = 0 -> return b
     */
    static long long conditional_select(long long a, long long b, int choice) {
        unsigned long long one = 1;
        unsigned long long mask = ((unsigned long long) choice - one);
        return (long long) ((~mask & (unsigned long long) a) | (mask & (unsigned long long) b));
    }

    static unsigned long long conditional_select(unsigned long long a, unsigned long long b, int choice) {
        unsigned long long one = 1;
        unsigned long long mask = ((unsigned long long) choice - one);
        return (~mask & a) | (mask & b);
    }

    /**
     * constant time selector
     * @param a
     * @param b
     * @param choice 0 or 1
     * @return choice = 1 -> a , choice = 0 -> return b
     */
    static unsigned __int128 conditional_select(unsigned __int128 a, unsigned __int128 b, int choice) {
        unsigned __int128 one = 1;
        unsigned __int128 mask = ((unsigned __int128) choice - one);
        return (~mask & a) | (mask & b);
    }

    /**
     * constant time selector
     * @param a
     * @param b
     * @param choice 0 or 1
     * @return choice = 1 -> a , choice = 0 -> return b
     */
    static byte_t conditional_select(byte_t a, byte_t b, int choice) {
        byte_t one = 1;
        byte_t mask = ((byte_t) choice - one);
        return (byte_t) ((~mask & a) | (mask & b));
    }

    /**
     * constant time comparator
     * @param left
     * @param right
     * @return left < right -> -1,  left = right -> 0, left > right -> 1
     */
    static signed char CTcmp(byte_t lhs, byte_t rhs) {
        unsigned short overflowing_iff_lt = (unsigned short) ((short) lhs - (short) rhs);
        unsigned short overflowing_iff_gt = (unsigned short) ((short) rhs - (short) lhs);
        signed char is_less_than = (signed char) -(overflowing_iff_lt >> 15); // -1 if self < other, 0 otherwise.
        signed char is_greater_than = (signed char) (overflowing_iff_gt >> 15); // 1 if self > other, 0 otherwise.
        signed char result = is_less_than + is_greater_than;
        return result;
    }

    static int CTcmp(Bid lhs, Bid rhs) {
        int res = 0;
        bool found = false;
        for (int i = ID_SIZE - 1; i >= 0; i--) {
            int cmpRes = CTcmp(lhs.id[i], rhs.id[i]);
            res = conditional_select(cmpRes, res, !found);
            found = conditional_select(true, found, !CTeq(cmpRes, 0) && !found);
        }
        return res;
    }

    static int CTcmp(std::array< byte_t, 16> lhs, std::array< byte_t, 16> rhs) {
        int res = 0;
        bool found = false;
        for (int i = 16 - 1; i >= 0; i--) {
            int cmpRes = CTcmp(lhs[i], rhs[i]);
            res = conditional_select(cmpRes, res, !found);
            found = conditional_select(true, found, !CTeq(cmpRes, 0) && !found);
        }
        return res;
    }

    /**
     * constant time selector
     * @param a
     * @param b
     * @param choice 0 or 1
     * @return choice = 1 -> a , choice = 0 -> return b
     */
    static Bid conditional_select(Bid a, Bid b, int choice) {
        Bid res;
        for (size_t i = 0; i < res.id.size(); i++) {
            res.id[i] = Bid::conditional_select(a.id[i], b.id[i], choice);
        }
        return res;
    }

    std::array< byte_t, ID_SIZE> id;
    Bid();
    constexpr Bid(const Bid & other)=default;
    Bid(long long value);
    Bid(std::array< byte_t, ID_SIZE> value);
    Bid(string value);
    bool operator!=(const Bid rhs) const;
    bool operator==(const Bid rhs) const;
    Bid& operator=(std::vector<byte_t> other);
    Bid& operator=(Bid const &other);
    bool operator<(const Bid& b) const;
    bool operator>(const Bid& b) const;
    bool operator<=(const Bid& b) const;
    bool operator>=(const Bid& b) const;
    long long getValue();
    void setToZero();
    bool isZero();
    void setValue(long long value);
    void setInfinity();
};



#endif /* BID_H */
