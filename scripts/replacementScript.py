"""
@file        replacementScript.py
@author      EmbedDB Team (See Authors.md)
@brief       This file is used by the github action to replace the variant 
             files with the custom ones created for our board. It was custom
             made for the runner and will not work locally.
@copyright   Copyright 2024
             EmbedDB Team
@par Redistribution and use in source and binary forms, with or without
 modification, are permitted provided that the following conditions are met:

@par 1.Redistributions of source code must retain the above copyright notice,
 this list of conditions and the following disclaimer.

@par 2.Redistributions in binary form must reproduce the above copyright notice,
 this list of conditions and the following disclaimer in the documentation
 and/or other materials provided with the distribution.

@par 3.Neither the name of the copyright holder nor the names of its contributors
 may be used to endorse or promote products derived from this software without
 specific prior written permission.

@par THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 POSSIBILITY OF SUCH DAMAGE.
"""

import os
from os.path import join

os.chdir("/home/runner")

# Location of the variant folder in the platformio installation on the runner machine
FRAMEWORK_DIR = join(
    ".platformio",
    "packages",
    "framework-arduino-samd-adafruit",
    "variants",
    "feather_m0",
)

# location of custom variant files in project
VARIANT_DIR = join("work", "iondb", "iondb", "variant")

# Code to remove old files and replace with new ones
variant_c_file_old = join(FRAMEWORK_DIR, "variant.cpp")
variant_h_file_old = join(FRAMEWORK_DIR, "variant.h")

os.remove(variant_c_file_old)
os.remove(variant_h_file_old)

variant_c_file_new = join(VARIANT_DIR, "variant.cpp")
variant_h_file_new = join(VARIANT_DIR, "variant.h")

os.replace(variant_c_file_new, variant_c_file_old)
os.replace(variant_h_file_new, variant_h_file_old)
