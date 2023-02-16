//
// Created by yulan on 22-7-8.
//

#ifndef MYSQL_TODO_H
#define MYSQL_TODO_H

// TODO 已知bug：
//  在 update t set id=id+10 (其中id是int型主键)的时候，会无线循环，原因是，假设原来有id=1，
//  在rnd_next的时候读到了它，并且通过update_row更新了它(id=11)，下一次rnd_next的时候就会又读到更新
//  的值(id=11)，因此造成无限循环

#endif  // MYSQL_TODO_H
