/**
 * 工具函数：确保数据可以被安全序列化
 */

export function ensureSerializable(data: any, maxDepth: number = 10): any {
	return cleanForSerialization(data, new WeakSet(), 0, maxDepth);
}

function cleanForSerialization(obj: any, seen: WeakSet<any>, depth: number, maxDepth: number): any {
	// 防止深度过深
	if (depth > maxDepth) {
		return '[Max depth reached]';
	}

	// 处理 null 和 undefined
	if (obj === null || obj === undefined) {
		return null;
	}

	// 处理基本类型
	const type = typeof obj;
	if (type !== 'object' && type !== 'function') {
		return obj;
	}

	// 跳过函数
	if (type === 'function') {
		return undefined;
	}

	// 处理日期
	if (obj instanceof Date) {
		return obj.toISOString();
	}

	// 处理正则表达式
	if (obj instanceof RegExp) {
		return obj.toString();
	}

	// 处理错误对象
	if (obj instanceof Error) {
		return {
			name: obj.name,
			message: obj.message,
			stack: obj.stack,
		};
	}

	// 检查循环引用
	if (seen.has(obj)) {
		return '[Circular Reference]';
	}
	seen.add(obj);

	// 处理数组
	if (Array.isArray(obj)) {
		return obj.map(item => cleanForSerialization(item, seen, depth + 1, maxDepth));
	}

	// 处理普通对象
	const cleaned: any = {};
	for (const key in obj) {
		if (obj.hasOwnProperty(key)) {
			try {
				const value = obj[key];
				const cleanedValue = cleanForSerialization(value, seen, depth + 1, maxDepth);
				if (cleanedValue !== undefined) {
					cleaned[key] = cleanedValue;
				}
			} catch (e) {
				// 忽略无法访问的属性
				cleaned[key] = '[Error accessing property]';
			}
		}
	}

	return cleaned;
}

/**
 * 验证对象是否可以序列化
 */
export function canSerialize(obj: any): boolean {
	try {
		JSON.stringify(obj);
		return true;
	} catch (e) {
		return false;
	}
}

/**
 * 获取序列化错误的详细信息
 */
export function getSerializationError(obj: any): string | null {
	try {
		JSON.stringify(obj);
		return null;
	} catch (e: any) {
		return e.message;
	}
}