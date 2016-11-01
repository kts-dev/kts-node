var options = {
  a: null,
  b: undefined,
  c: 0,
  d: false,
  e: '',
  f: {},
  g: [],
  h: ' '
};

function check(v) {
  console.log('--------');
  console.log(v);
  console.log(v
    ? 'yes'
    : 'no');
}


check(options.a);
check(options.b);
check(options.c);
check(options.d);
check(options.e);
check(options.f);
check(options.g);
check(options.h);
check(options.i);