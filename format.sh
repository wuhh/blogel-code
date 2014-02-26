cpp="$(find . -name '*.cpp' -o -name '*.h')"
for i in $cpp
do
    $(printf 'clang-format %s -style=WebKit -i' $i)
done
