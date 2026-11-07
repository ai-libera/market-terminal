TEMPLATE = app
TARGET = qt-market-terminal
QT += core gui widgets network
CONFIG += c++17
CONFIG -= app_bundle
DESTDIR = $$PWD/../build/bin
INCLUDEPATH += $$PWD/../common/qt
SOURCES = terminal.cpp
