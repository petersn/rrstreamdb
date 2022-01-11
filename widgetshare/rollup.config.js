// rollup.config.js
import typescript from '@rollup/plugin-typescript';
import { nodeResolve } from '@rollup/plugin-node-resolve';
//import resolve from 'rollup-plugin-node-resolve';
import commonjs from '@rollup/plugin-commonjs';

export default {
  input: 'src/index.tsx',
  output: {
    dir: 'dist',
    format: 'es',
  },
  plugins: [
	nodeResolve(),
	//resolve(),
	commonjs(),
    typescript(),
  ],
};
