#include "unity.h"

void setUp() {

}

void tearDown() {

}


int runUnityTests() {
    UNITY_BEGIN();
    //RUN_TEST();
    return UNITY_END();
}

#ifdef ARDUINO

void setup() {
    delay(2000);
    setupBoard();
    runUnityTests();
}

void loop() {}

#else

int main() {
    return runUnityTests();
}

#endif