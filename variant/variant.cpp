/*
  Copyright (c) 2014-2015 Arduino LLC.  All right reserved.

  This library is free software; you can redistribute it and/or
  modify it under the terms of the GNU Lesser General Public
  License as published by the Free Software Foundation; either
  version 2.1 of the License, or (at your option) any later version.

  This library is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
  See the GNU Lesser General Public License for more details.

  You should have received a copy of the GNU Lesser General Public
  License along with this library; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
*/

#include "variant.h"

const PinDescription g_APinDescription[] = {
/*
 +------------+------------------+--------+-----------------+--------+-----------------------+---------+---------+--------+--------+----------+----------+
 | Pin number |  Mem  Board pin  |  PIN   | Notes           | Peri.A |     Peripheral B      | Perip.C | Perip.D | Peri.E | Peri.F | Periph.G | Periph.H |
 |            |                  |        |                 |   EIC  | ADC |  AC | PTC | DAC | SERCOMx | SERCOMx |  TCCx  |  TCCx  |    COM   | AC/GLCK  |
 |            |                  |        |                 |(EXTINT)|(AIN)|(AIN)|     |     | (x/PAD) | (x/PAD) | (x/WO) | (x/WO) |          |          |
 +------------+------------------+--------+-----------------+--------+-----+-----+-----+-----+---------+---------+--------+--------+----------+----------+
 | 00         |    CHK_LED       |  PA11  |      error      |   11   |  19 |     | X03 |     |   0/03  |   2/03  | TCC1/1 | TCC0/3 | I2S/FS0  | GCLK_IO5 |
 | 01         |    PULSE_LED     |  PA23  |      pulse      |   07   |     |     | X11 |     |   3/01  |   5/01  |  TC4/1 | TCC0/5 | USB/SOF  | GCLK_IO7 |
 |            |     SD SPI       |        |                 |        |     |     |     |     |         |         |        |        |          |          |
 | 02         |                  |  PA04  | SD MOSI         |   04   |  04 |  00 | Y02 |     |         |  *0/00  | TCC0/0 | TCC3/2 |          |          |
 | 03         |                  |  PA05  | SD SCLK         |   05   |  05 |  01 | Y03 |     |         |  *0/01  | TCC0/1 | TCC3/3 |          |          |
 | 04         |                  |  PA06  | SD MISO         |   06   |  06 |  02 | Y04 |     |         |  *0/02  | TCC1/0 | TCC3/4 |          |          |
 | 05         |                  |  PA07  | SD SS           |   07   |  07 |  03 | Y05 |     |         |   0/03  | TCC1/1 | TCC3/5 | I2S/SD0  |          |
 |            |     AT SPI       |        |                 |        |     |     |     |     |         |         |        |        |          |          |                    
 | 06         |                  |  PA08  | AT MOSI         |   NMI  |  16 |     | X00 |     |   0/00  |  *2/00  | TCC0/0 | TCC1/2 | I2S/SD1  |          |
 | 07         |                  |  PA09  | AT SCLK         |   09   |  17 |     | X01 |     |   0/01  |  *2/01  | TCC0/1 | TCC1/3 | I2S/MCK0 |          |
 | 08         |                  |  PA10  | AT MISO         |   10   | *18 |     | X02 |     |   0/02  |  *2/02  | TCC1/0 | TCC0/2 | I2S/SCK0 | GCLK_IO4 |
 | 09         |                  |  PB10  | SS DB64         |   10   |     |     |     |     |         |   4/02  |  TC5/0 | TCC0/4 | I2S/MCK1 | GCLK_IO4 |
 | 10         |                  |  PB11  | SS DB32         |   11   |     |     |     |     |         |   4/03  |  TC5/1 | TCC0/5 | I2S/SCK1 | GCLK_IO5 |
 | 11         |                  |  PB12  | SS AT25         |   11   |     |     |     |     |   4/00  |         |  TC4/0 | TCC0/6 | I2S/FS1  | GCLK_IO6 |
 |            |   Special SPI    |        |                 |        |     |     |     |     |         |         |        |        |          |          |
 | 12         |                  |  PA12  | Special MOSI    |   12   |     |     |     |     |   2/00  |  *4/00  | TCC2/0 | TCC0/6 |          | AC/CMP0  |
 | 13         |                  |  PA13  | Special SCLK    |   13   |     |     |     |     |   2/01  |  *4/01  | TCC2/1 | TCC0/7 |          | AC/CMP1  |
 | 14         |                  |  PA14  | Special MISO    |   14   |     |     |     |     |   2/02  |  *4/02  |  TC3/0 | TCC0/4 |          | GCLK_IO0 |
 | 15         |                  |  PA15  | SS M3008        |   15   |     |     |     |     |   2/03  |   4/03  |  TC3/1 | TCC0/5 |          | GCLK_IO1 |
 | 16         |                  |  PB15  | SS CY15         |   15   |     |     | X15 |     |   4/03  |         |  TC5/1 |        |          | GCLK_IO1 |
 |            |     NOR SPI      |        |                 |        |     |     |     |     |         |         |        |        |          |          |
 | 17         | MOSI             |  PA16  | NOR MOSI        |   00   |     |     | X04 |     |  *1/00  |   3/00  | TCC2/0 | TCC0/6 |          | GCLK_IO2 |
 | 18         | SCLK             |  PA17  | NOR SCLK        |   01   |     |     | X05 |     |  *1/01  |   3/01  | TCC2/1 | TCC0/7 |          | GCLK_IO3 |
 | 19         | MISO             |  PA18  | NOR MISO        |   02   |     |     | X06 |     |  *1/02  |   3/02  |  TC3/0 | TCC0/2 |          | AC/CMP0  |
 | 20         |                  |  PB13  | SS MT25         |   13   |     |     | X13 |     |   4/01  |         |  TC4/1 | TCC0/7 |          | GCLK_IO7 |
 | 21         |                  |  PB14  | SS GD25         |   14   |     |     | X14 |     |   4/02  |         |  TC5/0 |        |          | GCLK_IO0 |
 |            |     NAND SP      |        |                 |        |     |     |     |     |         |         |        |        |          |          |
 | 22         | MOSI             |  PB16  | NAND MOSI       |   00   |     |     |     |     |  *5/00  |         |  TC6/0 | TCC0/4 | I2S/SD1  | GCLK_IO2 |
 | 23         | SCLK             |  PB17  | NAND SCLK       |   01   |     |     |     |     |  *5/01  |         |  TC6/1 | TCC0/5 | I2S/MCK0 | GCLK_IO3 |
 | 24         | MISO             |  PA20  | NAND MISO       |   04   |     |     | X08 |     |  *5/02  |   3/02  |  TC7/0 | TCC0/6 | I2S/SCK0 | GCLK_IO4 |
 | 25         |                  |  PA21  | SS GD5F         |   05   |     |     | X09 |     |   5/03  |   3/03  |  TC7/1 | TCC0/7 | I2S/FS0  | GCLK_IO5 |
 | 26         |                  |  PA22  | SS W25          |   06   |     |     | X10 |     |   3/00  |   5/00  |  TC4/0 | TCC0/4 |          | GCLK_IO6 |

 |            |       USB        |        |                 |        |     |     |     |     |         |         |        |        |          |          |
 | 27         |                  |  PA24  | USB N           |   12   |     |     |     |     |   3/02  |   5/02  |  TC5/0 | TCC1/2 | USB/DM   |          |
 | 28         |                  |  PA25  | USB P           |   13   |     |     |     |     |   3/03  |   5/03  |  TC5/1 | TCC1/3 | USB/DP   |          |
 |
 | 29         | PB00             |  PB00  |                 |  *00   |  8  |      | Y06 |     |         |   5/02  |  TC7/0 |        |          |          |
 | 30         | PB01             |  PB01  |                 |  *01   |  9  |      | Y07 |     |         |   5/03  |  TC7/2 |        |          |          |
 | 31         | PB02             |  PB02  |                 |  *02   |  10 |      | Y08 |     |         |   5/00  |  TC6/0 | TCC3/0 |          |          |
 | 32         | PB03             |  PB03  |                 |  *03   |  11 |      | Y01 |     |         |   5/01  |  TC6/1 |        |          |          |
 | 33         | PB04             |  PB04  |                 |  *04   |  12 |      | Y10 |     |         |         |        |        |          |          |
 | 34         | PB05             |  PB05  |                 |  *05   |  13 |      | Y11 |     |         |         |        |        |          |          |
 | 35         | PB06             |  PB06  |                 |  *06   |  14 |      | Y12 |     |         |         |        |        |          |          |
 | 36         | PB07             |  PB07  |                 |  *07   |  15 |      | Y13 |     |         |         |        |        |          |          |
 | 37         | PB08             |  PB08  |                 |  *08   |  02 |      | Y14 |     |         |         |        |        |          |          |
 | 38         | PB09             |  PB09  |                 |  *09   |  03 |      | Y15 |     |         |   4/01  |  TC4/1 | TCC3/7 |          |          |
 | 39         | PB30             |  PB30  |                 |  *14   |     |      |     |     |         |   5/00  | TCC0/0 | TCC1/2 |          |          |
 | 40         | PB31             |  PB31  |                 |  *15   |     |      |     |     |         |   5/01  | TCC0/1 | TCC1/3 |          |          |
 | 41         | PA02             |  PA02  |                 |  *02   |  00 |      | Y00 | OUT |         |         |        | TCC3/0 |          |          |
 | 42         | PA03             |  PA03  |                 |  *03   |  01 |      | Y01 |     |         |         |        | TCC3/1 |          |          |
 | 43         | PB22             |  PB22  |                 |   06   |     |      |     |     |         |   5/02  |  TC7/0 | TCC3/0 |          | GCLK_IO0 |
 | 44         | PB23             |  PB23  |                 |   07   |     |      |     |     |         |   5/03  |  TC7/1 | TCC3/1 |          | GCLK_IO1 |
 | 45         | PA27             |  PA27  |                 |   15   |     |      |     |     |         |         |        | TCC3/3 |          | GCLK_IO0 |
 |
 +---
 |
*/
  { PORTA, 11, PIO_DIGITAL,     (PIN_ATTR_DIGITAL                           ), No_ADC_Channel,   NOT_ON_PWM,     NOT_ON_TIMER,   EXTERNAL_INT_NONE },
  { PORTA, 23, PIO_DIGITAL,     (PIN_ATTR_DIGITAL                           ), No_ADC_Channel,   NOT_ON_PWM,     NOT_ON_TIMER,   EXTERNAL_INT_NONE },


                                                                                                                                                     // DIPO=2 DOPO=0   
  { PORTA, 4, PIO_SERCOM_ALT,   (PIN_ATTR_NONE                              ), No_ADC_Channel,  NOT_ON_PWM,     NOT_ON_TIMER,   EXTERNAL_INT_NONE }, // MOSI:   SERCOM0/PAD[0]
  { PORTA, 5, PIO_SERCOM_ALT,   (PIN_ATTR_NONE                              ), No_ADC_Channel,  NOT_ON_PWM,     NOT_ON_TIMER,   EXTERNAL_INT_NONE }, // SCLK:   SERCOM0/PAD[1]
  { PORTA, 6, PIO_SERCOM_ALT,   (PIN_ATTR_NONE                              ), No_ADC_Channel,  NOT_ON_PWM,     NOT_ON_TIMER,   EXTERNAL_INT_NONE }, // MISO:    SERCOM0/PAD[2]
  { PORTA, 7, PIO_DIGITAL,      (PIN_ATTR_NONE                              ), No_ADC_Channel,  NOT_ON_PWM,     NOT_ON_TIMER,   EXTERNAL_INT_NONE }, // SS:     as GPIO

                                                                                                                                                     // DIPO=2 DOPO=0   
  { PORTA,  8, PIO_SERCOM_ALT,  (PIN_ATTR_NONE                              ), No_ADC_Channel,  NOT_ON_PWM,     NOT_ON_TIMER,   EXTERNAL_INT_NONE }, // MOSI:   SERCOM2/PAD[0]
  { PORTA,  9, PIO_SERCOM_ALT,  (PIN_ATTR_NONE                              ), No_ADC_Channel,  NOT_ON_PWM,     NOT_ON_TIMER,   EXTERNAL_INT_NONE }, // SCLK:   SERCOM2/PAD[1]
  { PORTA, 10, PIO_SERCOM_ALT,  (PIN_ATTR_NONE                              ), No_ADC_Channel,  NOT_ON_PWM,     NOT_ON_TIMER,   EXTERNAL_INT_NONE }, // MISO:   SERCOM2/PAD[2]
  { PORTB, 10, PIO_DIGITAL,     (PIN_ATTR_NONE                              ), No_ADC_Channel,  NOT_ON_PWM,     NOT_ON_TIMER,   EXTERNAL_INT_NONE }, // SS:     as GPIO for AT45DB641
  { PORTB, 11, PIO_DIGITAL,     (PIN_ATTR_NONE                              ), No_ADC_Channel,  NOT_ON_PWM,     NOT_ON_TIMER,   EXTERNAL_INT_NONE }, // SS:     as GPIO for AT45DB32
  { PORTB, 12, PIO_DIGITAL,     (PIN_ATTR_NONE                              ), No_ADC_Channel,  NOT_ON_PWM,     NOT_ON_TIMER,   EXTERNAL_INT_NONE }, // SS:     as GPIO for AT25

                                                                                                                                                     // DIPO=2 DOPO=0 
  { PORTA, 12, PIO_SERCOM_ALT,  (PIN_ATTR_NONE                              ), No_ADC_Channel,  NOT_ON_PWM,     NOT_ON_TIMER,   EXTERNAL_INT_NONE }, // MOSI:   SERCOM4/PAD[0]
  { PORTA, 13, PIO_SERCOM_ALT,  (PIN_ATTR_NONE                              ), No_ADC_Channel,  NOT_ON_PWM,     NOT_ON_TIMER,   EXTERNAL_INT_NONE }, // SCLK:   SERCOM4/PAD[1]
  { PORTA, 14, PIO_SERCOM_ALT,  (PIN_ATTR_NONE                              ), No_ADC_Channel,  NOT_ON_PWM,     NOT_ON_TIMER,   EXTERNAL_INT_NONE }, // MISO:   SERCOM4/PAD[2] 
  { PORTA, 15, PIO_DIGITAL,     (PIN_ATTR_NONE                              ), No_ADC_Channel,  NOT_ON_PWM,     NOT_ON_TIMER,   EXTERNAL_INT_NONE }, // SS:     as GPIO for M3008
  { PORTB, 15, PIO_DIGITAL,     (PIN_ATTR_NONE                              ), No_ADC_Channel,  NOT_ON_PWM,     NOT_ON_TIMER,   EXTERNAL_INT_NONE }, // SS:     as GPIO for CY15

                                                                                                                                                     // DIPO=2 DOPO=0 
  { PORTA, 16, PIO_SERCOM,      (PIN_ATTR_NONE                              ), No_ADC_Channel,  NOT_ON_PWM,     NOT_ON_TIMER,   EXTERNAL_INT_NONE }, // MOSI:   SERCOM1/PAD[0]
  { PORTA, 17, PIO_SERCOM,      (PIN_ATTR_NONE                              ), No_ADC_Channel,  NOT_ON_PWM,     NOT_ON_TIMER,   EXTERNAL_INT_NONE }, // SCLK:   SERCOM1/PAD[1]
  { PORTA, 18, PIO_SERCOM,      (PIN_ATTR_NONE                              ), No_ADC_Channel,  NOT_ON_PWM,     NOT_ON_TIMER,   EXTERNAL_INT_NONE }, // MISO:   SERCOM1/PAD[2]
  { PORTB, 13, PIO_DIGITAL,     (PIN_ATTR_NONE                              ), No_ADC_Channel,  NOT_ON_PWM,     NOT_ON_TIMER,   EXTERNAL_INT_NONE }, // SS:     as GPIO for MT25
  { PORTB, 14, PIO_DIGITAL,     (PIN_ATTR_NONE                              ), No_ADC_Channel,  NOT_ON_PWM,     NOT_ON_TIMER,   EXTERNAL_INT_NONE }, // SS:     as GPIO for GD25

                                                                                                                                                     // DIPO=2 DOPO=0 
  { PORTB, 16, PIO_SERCOM,      (PIN_ATTR_NONE                              ), No_ADC_Channel,  NOT_ON_PWM,     NOT_ON_TIMER,   EXTERNAL_INT_NONE }, // MOSI:   SERCOM5/PAD[0]
  { PORTB, 17, PIO_SERCOM,      (PIN_ATTR_NONE                              ), No_ADC_Channel,  NOT_ON_PWM,     NOT_ON_TIMER,   EXTERNAL_INT_NONE }, // SCLK:   SERCOM5/PAD[1]
  { PORTA, 20, PIO_SERCOM,      (PIN_ATTR_NONE                              ), No_ADC_Channel,  NOT_ON_PWM,     NOT_ON_TIMER,   EXTERNAL_INT_NONE }, // MISO:   SERCOM5/PAD[2]
  { PORTA, 21, PIO_DIGITAL,     (PIN_ATTR_NONE                              ), No_ADC_Channel,  NOT_ON_PWM,     NOT_ON_TIMER,   EXTERNAL_INT_NONE }, // SS:     as GPIO for GD5F
  { PORTA, 22, PIO_DIGITAL,     (PIN_ATTR_NONE                              ), No_ADC_Channel,  NOT_ON_PWM,     NOT_ON_TIMER,   EXTERNAL_INT_NONE }, // SS:     as GPIO for W25

  { PORTA, 24, PIO_COM,         (PIN_ATTR_NONE                              ), No_ADC_Channel,  NOT_ON_PWM,     NOT_ON_TIMER,   EXTERNAL_INT_NONE }, // USB/DM
  { PORTA, 25, PIO_COM,         (PIN_ATTR_NONE                              ), No_ADC_Channel,  NOT_ON_PWM,     NOT_ON_TIMER,   EXTERNAL_INT_NONE }, // USB/DP
  
  { PORTB,  0, PIO_DIGITAL,     (PIN_ATTR_DIGITAL                           ), No_ADC_Channel,  NOT_ON_PWM,     NOT_ON_TIMER,   EXTERNAL_INT_0    },
  { PORTB,  1, PIO_DIGITAL,     (PIN_ATTR_DIGITAL                           ), No_ADC_Channel,  NOT_ON_PWM,     NOT_ON_TIMER,   EXTERNAL_INT_1    },
  { PORTB,  2, PIO_DIGITAL,     (PIN_ATTR_DIGITAL                           ), No_ADC_Channel,  NOT_ON_PWM,     NOT_ON_TIMER,   EXTERNAL_INT_2    },
  { PORTB,  3, PIO_DIGITAL,     (PIN_ATTR_DIGITAL                           ), No_ADC_Channel,  NOT_ON_PWM,     NOT_ON_TIMER,   EXTERNAL_INT_3    },
  { PORTB,  4, PIO_DIGITAL,     (PIN_ATTR_DIGITAL                           ), No_ADC_Channel,  NOT_ON_PWM,     NOT_ON_TIMER,   EXTERNAL_INT_4    },
  { PORTB,  5, PIO_DIGITAL,     (PIN_ATTR_DIGITAL                           ), No_ADC_Channel,  NOT_ON_PWM,     NOT_ON_TIMER,   EXTERNAL_INT_5    },
  { PORTB,  6, PIO_DIGITAL,     (PIN_ATTR_DIGITAL                           ), No_ADC_Channel,  NOT_ON_PWM,     NOT_ON_TIMER,   EXTERNAL_INT_6    },
  { PORTB,  7, PIO_DIGITAL,     (PIN_ATTR_DIGITAL                           ), No_ADC_Channel,  NOT_ON_PWM,     NOT_ON_TIMER,   EXTERNAL_INT_7    },
  { PORTB,  8, PIO_DIGITAL,     (PIN_ATTR_DIGITAL                           ), No_ADC_Channel,  NOT_ON_PWM,     NOT_ON_TIMER,   EXTERNAL_INT_8    },
  { PORTB,  9, PIO_DIGITAL,     (PIN_ATTR_DIGITAL                           ), No_ADC_Channel,  NOT_ON_PWM,     NOT_ON_TIMER,   EXTERNAL_INT_9    },
  { PORTB,  30,PIO_DIGITAL,     (PIN_ATTR_DIGITAL                           ), No_ADC_Channel,  NOT_ON_PWM,     NOT_ON_TIMER,   EXTERNAL_INT_14   },
  { PORTB,  31,PIO_DIGITAL,     (PIN_ATTR_DIGITAL                           ), No_ADC_Channel,  NOT_ON_PWM,     NOT_ON_TIMER,   EXTERNAL_INT_15   },
  { PORTA,  2, PIO_DIGITAL,     (PIN_ATTR_DIGITAL                           ), No_ADC_Channel,  NOT_ON_PWM,     NOT_ON_TIMER,   EXTERNAL_INT_2    },
  { PORTA,  3, PIO_DIGITAL,     (PIN_ATTR_DIGITAL                           ), No_ADC_Channel,  NOT_ON_PWM,     NOT_ON_TIMER,   EXTERNAL_INT_3    },  
  { PORTB, 22, PIO_DIGITAL,     (PIN_ATTR_DIGITAL                           ), No_ADC_Channel,  NOT_ON_PWM,     NOT_ON_TIMER,   EXTERNAL_INT_NONE },  
  { PORTB, 23, PIO_DIGITAL,     (PIN_ATTR_DIGITAL                           ), No_ADC_Channel,  NOT_ON_PWM,     NOT_ON_TIMER,   EXTERNAL_INT_NONE },  
  { PORTA, 27, PIO_DIGITAL,     (PIN_ATTR_DIGITAL                           ), No_ADC_Channel,  NOT_ON_PWM,     NOT_ON_TIMER,   EXTERNAL_INT_NONE },

  { PORTA, 28, PIO_COM, PIN_ATTR_NONE, No_ADC_Channel, NOT_ON_PWM, NOT_ON_TIMER, EXTERNAL_INT_NONE }, // USB Host enable

};

extern "C" {
    unsigned int PINCOUNT_fn() {
        return (sizeof(g_APinDescription) / sizeof(g_APinDescription[0]));
    }
}

const void* g_apTCInstances[TCC_INST_NUM + TC_INST_NUM]={ TCC0, TCC1, TCC2, TC3, TC4, TC5 };

// Multi-serial objects instantiation
SERCOM sercom0(SERCOM0);
SERCOM sercom1(SERCOM1);
SERCOM sercom2(SERCOM2);
//SERCOM sercom3(SERCOM3);
SERCOM sercom4(SERCOM4);
SERCOM sercom5(SERCOM5);

// Serial1
//Uart Serial1(&sercom5, PIN_SERIAL1_RX, PIN_SERIAL1_TX, PAD_SERIAL1_RX, PAD_SERIAL1_TX);
/*
void SERCOM5_Handler()
{
  Serial1.IrqHandler();
}
*/